from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import boto3

default_args = {"retries": 1, "retry_delay": timedelta(minutes=5)}

S3_BUCKET = "kaggle-retail-analytics-dataset"
S3_PREFIX = "sales_data/"
INGESTION_LOG = "file_ingestion_log"
STAGING_TABLE = "sales_staging"

# List all S3 files recursively
def list_all_s3_files(ti):
    s3 = boto3.client("s3")
    files = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith("/"):  # skip folders
                files.append(obj["Key"])
    print("Files found in S3:", files)
    return files

# Filter files not already loaded in Snowflake
def filter_new_files(ti):
    hook = SnowflakeHook(snowflake_conn_id="my_snowflake_conn")
    df = hook.get_pandas_df(f"SELECT filename FROM {INGESTION_LOG}")
    already_loaded = set(df["FILENAME"].tolist())
    files = ti.xcom_pull(task_ids="list_s3_files") or []
    new_files = [f for f in files if f not in already_loaded]
    print("New files to load:", new_files)
    return new_files

# Build SQL statements for Snowflake
def build_sql_statements(ti):
    files = ti.xcom_pull(task_ids="filter_new_files") or []
    sqls = []
    for f in files:
        sqls.append(f"""
            COPY INTO {STAGING_TABLE}
            FROM @my_s3_stage/{f}
            FILE_FORMAT = (FORMAT_NAME = my_csv_format);
        """)
        sqls.append(f"""
            INSERT INTO {INGESTION_LOG} VALUES ('{f}', CURRENT_TIMESTAMP);
        """)
    return "; ".join(sqls)

# DAG definition
with DAG(
    "s3_to_snowflake_staging_dag",
    default_args=default_args,
    start_date=datetime(2025, 9, 2),
    schedule_interval="@hourly",
    catchup=False,
) as dag:

    list_s3_files = PythonOperator(
        task_id="list_s3_files",
        python_callable=list_all_s3_files,
    )

    filter_files = PythonOperator(
        task_id="filter_new_files",
        python_callable=filter_new_files,
    )

    build_sqls = PythonOperator(
        task_id="build_sqls",
        python_callable=build_sql_statements,
    )

    load_to_snowflake = SnowflakeOperator(
        task_id="load_to_snowflake",
        snowflake_conn_id="my_snowflake_conn",
        sql="{{ ti.xcom_pull(task_ids='build_sqls') }}",
    )

    list_s3_files >> filter_files >> build_sqls >> load_to_snowflake
