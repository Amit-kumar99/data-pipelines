from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

default_args = {"retries": 1, "retry_delay": timedelta(minutes=5)}

S3_BUCKET = "kaggle-retail-analytics-dataset"
S3_PREFIX = "sales_data/"
INGESTION_LOG = "file_ingestion_log"
STAGING_TABLE = "sales_staging"

def build_sql_statements(ti):
    files = ti.xcom_pull(task_ids="filter_new_files")
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
    return sqls

with DAG(
    "s3_to_snowflake_staging_dag",
    default_args=default_args,
    start_date=datetime(2025, 9, 2),
    schedule_interval="@hourly",
    catchup=False,
) as dag:

    # 1. List S3 files
    list_s3_files = S3ListOperator(
        task_id="list_s3_files",
        bucket=S3_BUCKET,
        prefix=S3_PREFIX,
        aws_conn_id="aws_default",
    )

    # 2. Filter files not already loaded
    def filter_new_files(ti):
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        hook = SnowflakeHook(snowflake_conn_id="my_snowflake_conn")
        df = hook.get_pandas_df(f"SELECT filename FROM {INGESTION_LOG}")
        already_loaded = set(df["FILENAME"].tolist())
        files = ti.xcom_pull(task_ids="list_s3_files")
        return [f for f in files if f not in already_loaded]

    filter_files = PythonOperator(
        task_id="filter_new_files",
        python_callable=filter_new_files,
    )

    # 3. Build SQL for each file and run via SnowflakeOperator
    build_sqls = PythonOperator(
        task_id="build_sqls",
        python_callable=build_sql_statements,
    )

    load_to_snowflake = SnowflakeOperator(
        task_id="load_to_snowflake",
        snowflake_conn_id="my_snowflake_conn",
        sql="{{ ti.xcom_pull(task_ids='build_sqls') | tojson }}",
    )

    list_s3_files >> filter_files >> build_sqls >> load_to_snowflake
