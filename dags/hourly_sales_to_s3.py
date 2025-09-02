from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import boto3

# Default DAG arguments
default_args = {
    'owner': 'amit',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Upload function
def upload_sales_to_s3():
    # Load your CSV file
    df = pd.read_csv("/home/ubuntu/data/sales.csv")

    # Add partition columns (year/month/day from sale_date column)
    df['sale_date'] = pd.to_datetime(df['sale_date'])
    df['year'] = df['sale_date'].dt.year
    df['month'] = df['sale_date'].dt.month
    df['day'] = df['sale_date'].dt.day

    # S3 client
    s3 = boto3.client("s3")
    bucket = "my-sales-data-bucket"

    # Loop through partitions and upload each subset
    for (year, month, day), subset in df.groupby(["year", "month", "day"]):
        key = f"sales_data/year={year}/month={month}/day={day}/sales.csv"
        temp_file = f"/tmp/sales_{year}_{month}_{day}.csv"

        subset.to_csv(temp_file, index=False)
        s3.upload_file(temp_file, bucket, key)

        print(f"✅ Uploaded {key}")

# Define the DAG
with DAG(
    dag_id='sales_to_s3_dag',
    default_args=default_args,
    start_date=datetime(2025, 9, 2),
    schedule_interval=None,  # Run only when triggered
    catchup=False
) as dag:

    upload_task = PythonOperator(
        task_id='upload_sales_to_s3',
        python_callable=upload_sales_to_s3
    )

    upload_task
