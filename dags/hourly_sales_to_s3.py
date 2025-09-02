from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import boto3
import os
import logging

# Default DAG arguments
default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Upload function
def upload_sales_to_s3():
    # Load your CSV file (absolute path)
    csv_path = "/home/ssm-user/sales-dataset.csv"
    df = pd.read_csv(csv_path)

    # Convert 'Date' column to datetime and add partition columns
    df['Date'] = pd.to_datetime(df['Date'], format="%d/%m/%Y")
    df['year'] = df['Date'].dt.year
    df['month'] = df['Date'].dt.month
    df['day'] = df['Date'].dt.day

    # S3 client
    s3 = boto3.client("s3")
    bucket = "kaggle-retail-analytics-dataset"

    # Loop through partitions and upload each subset
    for (year, month, day), subset in df.groupby(["year", "month", "day"]):
        key = f"sales_data/year={year}/month={month:02}/day={day:02}/sales.csv"
        temp_file = f"/tmp/sales_{year}_{month:02}_{day:02}.csv"

        subset.to_csv(temp_file, index=False)
        s3.upload_file(temp_file, bucket, key)

        logging.info(f"Uploaded {key} to S3")
        os.remove(temp_file)

# Define the DAG
with DAG(
    dag_id='sales_to_s3_dag',
    default_args=default_args,
    start_date=datetime(2025, 9, 2),
    schedule_interval='@once',  # Run only when triggered
    catchup=False
) as dag:

    upload_task = PythonOperator(
        task_id='upload_sales_to_s3_task',
        python_callable=upload_sales_to_s3
    )

    upload_task
