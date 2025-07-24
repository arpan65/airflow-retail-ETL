from airflow.sdk import asset, task, dag
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from datetime import datetime
from sqlalchemy import create_engine
import pandas as pd
import boto3
import pyarrow as pa
import pyarrow.parquet as pq

# ----------------- Configuration -----------------
POSTGRES_CONN_STRING = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
RAW_CSV_PATH = "/opt/airflow/data/raw/events.csv"
SUMMARY_PARQUET_PATH = "/opt/airflow/data/raw/events_summary.parquet"
S3_BUCKET = "airflow-retail-stage"
S3_KEY = "events_summary.parquet"

def get_engine():
    return create_engine(POSTGRES_CONN_STRING)

# ----------------- Asset 1 -----------------
@asset(schedule="@daily")
def load_raw_to_staging():
    df = pd.read_csv(RAW_CSV_PATH).head(1000)
    df = df[df["event"].isin(["view", "addtocart", "transaction"])]
    df = df.dropna(subset=["visitorid", "itemid"])
    df["event_time"] = pd.to_datetime(df["timestamp"], unit="ms")
    engine = get_engine()
    df.to_sql("staging_events", con=engine, if_exists="append", index=False, method="multi")
    print(f"✅ Staged {len(df)} records.")
    return True

# ----------------- Asset 2 -----------------
@asset(schedule=load_raw_to_staging)
def load_events_summary():
    engine = get_engine()
    df = pd.read_sql("""
        SELECT DATE(event_time) AS event_date, event, COUNT(*) AS event_count
        FROM staging_events
        GROUP BY event_date, event
        ORDER BY event_date, event;
    """, con=engine)
    pq.write_table(pa.Table.from_pandas(df), SUMMARY_PARQUET_PATH)
    print(f"✅ Summary written to {SUMMARY_PARQUET_PATH}")
    return True

# ----------------- Task: Upload to S3 -----------------
@task
def upload_to_s3():
    try:
        s3 = boto3.client("s3", region_name="us-east-1")
        with open(SUMMARY_PARQUET_PATH, "rb") as f:
            s3.upload_fileobj(f, Bucket=S3_BUCKET, Key=S3_KEY)
        print(f"✅ Uploaded to s3://{S3_BUCKET}/{S3_KEY}")
        return True
    except Exception as e:
        print(f"❌ Upload failed: {e}")
        return False

# ----------------- Branching & Notifications -----------------
@task.branch
def choose_branch(upload_success: bool):
    return "task_complete_notification" if upload_success else "task_failed_notification"

@task
def task_complete_notification():
    print("🎉 Upload succeeded.")

@task
def task_failed_notification():
    print("⚠️ Upload failed.")

# ----------------- DAG -----------------
@dag(
    schedule="@daily",
    tags=["retail", "airflow3"]
)
def retail_pipeline():
    # Only orchestrate tasks here (assets are managed separately)
    upload_success = upload_to_s3()
    decision = choose_branch(upload_success)
    task_complete_notification().set_upstream(decision)
    task_failed_notification().set_upstream(decision)

retail_pipeline()
