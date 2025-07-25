# import necessary libraries
from airflow.sdk import dag, task, asset
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from io import BytesIO
from sqlalchemy import create_engine
from datetime import datetime, date, timedelta
from airflow.hooks.base import BaseHook
import pandas as pd
import boto3
import pyarrow as pa
import pyarrow.parquet as pq

CURRENT_DATE = '2019-10-23'
S3_BUCKET = "airflow-retail-stage"
S3_BUCKET_MART = "airflow-retail-mart"
S3_PREFIX = "Day_Wise"

# method to connect with staging db, note: we already created a connection called 'postgres'
def get_engine():
    conn = BaseHook.get_connection("postgres")
    conn_str = f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    return create_engine(conn_str)

def preprocess_raw_csv(df):
    ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    df['category_code'] = df['category_code'].fillna('Unknown')
    df['brand'] = df['brand'].fillna('Generic')
    df['loaded_at'] = [ts] * len(df)
    return df

# asset - runs everyday, reads raw data from s3
@asset(schedule="@daily")
def load_raw_data_from_s3():
    # datestamp of current run date
    #ds = context["ds"]
    s3 = boto3.client("s3", region_name="us-east-1")
    s3_key = f"{S3_PREFIX}/{CURRENT_DATE}/event.csv"
    try:
        print(f"Reading file s3://{S3_BUCKET}/{s3_key}")
        response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        df = pd.read_csv(BytesIO(response["Body"].read()))
        df['last_loaded'] = []
        print("Total data count:", len(df))
        if len(df) > 0:
            engine = get_engine()
            df_cleaned = preprocess_raw_csv(df)
            df_cleaned.to_sql("staging_events", engine, if_exists="append", index=False, method="multi")
            print(f"✅ Loaded {len(df_cleaned)} records to staging_events.")
    except Exception as e:
        print("Exception occurred", e)

# asset - loads data to fact table from stage
@asset(schedule=load_raw_data_from_s3)
def load_fact_table():
    engine = get_engine()
    query = """
        SELECT 
            event_date,
            event_type,
            product_id,
            category_id,
            brand,
            user_id,
            user_session,
            COUNT(*) AS total_events,
            SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) AS total_revenue
        FROM staging_events 
        GROUP BY 1, 2, 3, 4, 5, 6, 7
    """
    try:
        df = pd.read_sql(query, con=engine)
        df.to_sql("fact_events", con=engine, if_exists="append", index=False, method="multi")
    except Exception as e:
        print("Exception occurred", e)

@asset(schedule=load_raw_data_from_s3)
def load_dim_user():
    engine = get_engine()
    df = pd.read_sql_query("""
        SELECT DISTINCT user_id, user_session
        FROM staging_events
        WHERE user_id IS NOT NULL 
    """, con=engine)
    df.to_sql("dim_user", con=engine, if_exists="replace", index=False)
    print(f"✅ Loaded {len(df)} users into dim_user")

@asset(schedule=load_raw_data_from_s3)
def load_dim_product():
    engine = get_engine()
    df = pd.read_sql_query("""
        SELECT DISTINCT product_id, category_id, category_code, brand, price
        FROM staging_events
        WHERE product_id IS NOT NULL
    """, con=engine)

    df['category_code'] = df['category_code'].fillna('other')

    # Split category_code into category levels
    cat, subcat1, subcat2 = [], [], []
    for code in df['category_code']:
        parts = code.split('.')
        cat.append(parts[0] if len(parts) > 0 else 'na')
        subcat1.append(parts[1] if len(parts) > 1 else 'na')
        subcat2.append(parts[2] if len(parts) > 2 else 'na')

    df['category'] = cat
    df['sub_category1'] = subcat1
    df['sub_category2'] = subcat2

    df.drop_duplicates(subset=["product_id"], inplace=True)
    df.to_sql("dim_product", con=engine, if_exists="replace", index=False)
    print(f"✅ Loaded {len(df)} products into dim_product")

@asset(schedule="@once")
def load_dim_date():
    engine = get_engine()
    dates = pd.date_range(start="2019-10-01", end="2020-03-31")
    df = pd.DataFrame({
        "date": dates,
        "day": dates.day,
        "month": dates.month,
        "year": dates.year,
        "weekday": dates.weekday,
        "week": dates.isocalendar().week,
    })
    df.to_sql("dim_date", con=engine, if_exists="replace", index=False)
    print(f"✅ Loaded {len(df)} rows into dim_date")

@task
def agg_daily_metrics_to_s3(execution_date: str):
    engine = get_engine()

    # 1. Daily Revenue Summary
    df_revenue = pd.read_sql_query(
        f"""
        SELECT event_date,
            SUM(total_revenue) AS revenue,
            COUNT(DISTINCT user_id) AS unique_users,
            SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchases,
            SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS carts,
            SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS views
        FROM fact_events
        WHERE event_date = '{execution_date}'
        GROUP BY event_date
        """, con=engine
    )

    # Add conversion metrics
    if not df_revenue.empty:
        df_revenue["cart_rate"] = df_revenue["carts"] / df_revenue["views"]
        df_revenue["purchase_rate"] = df_revenue["purchases"] / df_revenue["views"]

    # 2. Daily Category/Brand Funnel
    df_cat_brand = pd.read_sql_query(
        f"""
        SELECT 
            event_date,
            category_id,
            brand,
            SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS views,
            SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS carts,
            SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchases,
            SUM(CASE WHEN event_type = 'purchase' THEN total_revenue ELSE 0 END) AS revenue
        FROM fact_events
        WHERE event_date = '{execution_date}'
        GROUP BY event_date, category_id, brand
        """, con=engine
    )

    # 3. Top Brands/Categories by Revenue
    df_top = df_cat_brand.sort_values("revenue", ascending=False).head(10)

    # 4. Write all to S3
    s3 = boto3.client("s3", region_name="us-east-1")

    def upload_df(df, name):
        if not df.empty:
            local_path = f"/tmp/{name}_{execution_date}.parquet"
            s3_key = f"aggregates/{name}/dt={execution_date}/data.parquet"
            pq.write_table(pa.Table.from_pandas(df), local_path)
            s3.upload_file(local_path, S3_BUCKET_MART, s3_key)
            print(f"✅ Uploaded {name} to s3://{S3_BUCKET_MART}/{s3_key}")

    upload_df(df_revenue, "daily_revenue_summary")
    upload_df(df_cat_brand, "daily_funnel_by_brand")
    upload_df(df_top, "top_brands_by_revenue")


@dag(tags=["retail", "aggregation", "mart"])
def retail_pipeline_mart():
    agg_daily_metrics_to_s3(CURRENT_DATE)

retail_pipeline_mart()
