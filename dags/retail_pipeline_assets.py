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

# To illustrate daily data load based on the dataset 
CURRENT_DATE = '2019-10-05'
S3_BUCKET = "airflow-retail-stage"
S3_BUCKET_MART = "airflow-retail-mart"
S3_PREFIX = "Day_Wise"

# method to connect with staging db, note: we already created a connection called 'postgres'
def get_engine():
    conn = BaseHook.get_connection("postgres")
    conn_str = f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    return create_engine(conn_str)

# method to preprocess the raw data
def preprocess_raw_csv(df):
    ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    df['category_code'] = df['category_code'].fillna('Unknown')
    df['brand'] = df['brand'].fillna('Generic')
    df['loaded_at'] = [ts] * len(df)
    return df

# asset - runs everyday, reads raw data from s3
@asset(schedule="@daily")
def load_raw_data_from_s3():
    s3 = boto3.client("s3", region_name="us-east-1")
    s3_key = f"{S3_PREFIX}/{CURRENT_DATE}/event.csv"
    try:
        print(f"Reading file s3://{S3_BUCKET}/{s3_key}")
        response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        df = pd.read_csv(BytesIO(response["Body"].read()))
        if df.empty:
            raise ValueError("The file is empty — aborting to prevent bad load.")
        df["event_date"] = pd.to_datetime(CURRENT_DATE).date()
        df["loaded_at"] = pd.to_datetime("now").date()
        df_cleaned = preprocess_raw_csv(df)
        engine = get_engine()
        df_cleaned.to_sql("staging_events", engine, if_exists="replace", index=False, method="multi")
        print(f"✅ Loaded {len(df_cleaned)} records to staging_events.")
    
    except Exception as e:
        print("❌ Exception while loading raw data from S3:", e)
        raise e 

# asset - loads data to fact table from stage
@asset(schedule=load_raw_data_from_s3)
def load_fact_table():
    try:
        engine = get_engine()
        query = f"""
            SELECT 
                event_date,
                event_type,
                product_id,
                user_id,
                COUNT(*) AS total_events,
                SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) AS total_revenue
            FROM staging_events WHERE event_date = '{CURRENT_DATE}'
            GROUP BY 1, 2, 3, 4
        """
        df = pd.read_sql(query, con=engine)
        df.to_sql("fact_events", con=engine, if_exists="replace", index=False, method="multi")
        print(f"✅ Loaded fact_events for {CURRENT_DATE}")
    except Exception as e:
        print("❌ Exception in load_fact_table:", e)
        raise e

@asset(schedule=load_raw_data_from_s3)
def load_dim_user():
    try:
        engine = get_engine()
        df = pd.read_sql_query(f"""
            SELECT DISTINCT user_id, user_session
            FROM staging_events
            WHERE event_date = '{CURRENT_DATE}' AND user_id IS NOT NULL 
        """, con=engine)
        df['updated_at'] = [CURRENT_DATE] * len(df)
        df.to_sql("dim_user", con=engine, if_exists="replace", index=False)
        print(f"✅ Loaded {len(df)} users into dim_user")
    except Exception as e:
        print("❌ Exception in load_dim_user:", e)
        raise e

@asset(schedule=load_raw_data_from_s3)
def load_dim_product():
    try:
        engine = get_engine()
        df = pd.read_sql_query(f"""
            SELECT DISTINCT product_id, category_id, category_code, brand, price
            FROM staging_events
            WHERE event_date = '{CURRENT_DATE}' AND product_id IS NOT NULL
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
    except Exception as e:
        print("❌ Exception in load_dim_product:", e)
        raise

@asset(schedule="@once")
def load_dim_date():
    try:
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
    except Exception as e:
        print("❌ Exception in load_dim_date:", e)
        raise e
