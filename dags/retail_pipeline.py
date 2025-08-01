# import necessary libraries
from airflow.sdk import dag, task, asset
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator

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


@task
def agg_daily_metrics_to_s3(execution_date: str):
    try:
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

        # 2. Daily Brand/Category Funnel
        df_cat_brand = pd.read_sql_query(
        f"""
            SELECT 
            event_date,
            p.brand,
            p.category_code,
            SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS views,
            SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS carts,
            SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchases,
            SUM(CASE WHEN event_type = 'purchase' THEN total_revenue ELSE 0 END) AS revenue
            FROM fact_events e 
            JOIN dim_product p ON e.product_id = p.product_id
            WHERE event_date = '{execution_date}'
            GROUP BY event_date, p.brand, p.category_code

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
        return True
    except Exception as e:
        print("❌ Exception in agg_daily_metrics_to_s3:", e)

@dag(tags=["retail", "aggregation", "mart"])
def retail_pipeline_mart():
    update_agg = agg_daily_metrics_to_s3(CURRENT_DATE)
    refresh_data_catelogue = GlueCrawlerOperator(
        task_id='run_glue_crawler',
        config={
            "Name": "retail-parquet-crawler"
        },
        aws_conn_id='aws',
        wait_for_completion=True,
        retry_delay=timedelta(minutes=5),
    )
    update_agg >> refresh_data_catelogue

retail_pipeline_mart()