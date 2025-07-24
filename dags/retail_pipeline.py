# import necessary libraries
from airflow.sdk import dag, task,asset
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
#from airflow.providers.common.aws.operators.s3 import 
from io import BytesIO
from sqlalchemy import create_engine
from datetime import datetime
from airflow.hooks.base import BaseHook
import random
import pandas as pd
import os
import boto3


CURRENT_DATE='2019-10-23'
S3_BUCKET = "airflow-retail-stage"
S3_PREFIX ="Day_Wise"

# method to connect with staging db, note: we already created a connection called 'postgres'
def get_engine():
    conn = BaseHook.get_connection("postgres")
    conn_str = f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    return create_engine(conn_str)

def preprocess_raw_csv(df):
    ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    df['category_code']=df['category_code'].fillna('Unknown')
    df['brand']=df['brand'].fillna('Generic')
    df['loaded_at']=[ts]*len(df)
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
        print("Total data count:",len(df)) 
        if len(df)>0: 
            engine = get_engine()
            df_cleaned = preprocess_raw_csv(df)
            df.to_sql("staging_events", engine, if_exists="append", index=False, method="multi")
            print(f"✅ Loaded {len(df)} records to staging_events.")
    except Exception as e:
        print("Exception occured",e)    

# asset - loads data to fact table from stage
@asset(schedule=load_raw_data_from_s3)
def load_fact_table():
    engine = get_engine()
    query="""
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
        df = pd.read_sql(query,con=engine)
        df.to_sql("fact_events",con=engine,if_exists="append",index=False,method="multi")
    except Exception as e:
        print("Exception occured",e)    

@asset(schedule=load_raw_data_from_s3)   
def load_dim_user():
    engine = get_engine()
    df = pd.read_sql_query("""
        SELECT DISTINCT user_id, user_session
        FROM staging_events
        WHERE user_id IS NOT NULL
    """, con=engine)
    #df.drop_duplicates(subset=["user_id"], inplace=True)
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

    df.drop_duplicates(subset=["product_id"], inplace=True)
    df.to_sql("dim_product", con=engine, if_exists="replace", index=False)
    print(f"✅ Loaded {len(df)} products into dim_product")


@asset(schedule="@once")
def load_dim_date():
    from datetime import timedelta
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







