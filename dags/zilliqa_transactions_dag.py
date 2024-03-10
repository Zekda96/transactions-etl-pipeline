"""
Airflow Pipeline that extracts crypto transactions data,
transforms it and stores it in a LocalStack S3 bucket.
"""

# Utils
from datetime import timedelta, datetime

# Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator

# Tasks
from extract_and_transform_data import query_data
from load_data import load_data

default_args = {
    'owner': 'Daniel Granja',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=5),
    'email': ['daniel_granja_96@hotmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
}

# Schedule interval is set to '@once' so the DAG runs automatically on start.
yesterday = datetime.now() - timedelta(days=1)

with DAG(
    dag_id='zilliqa-transactions-DAG',
    default_args=default_args,
    description='ETL - Crypto transactions data to S3 bucket',
    schedule_interval="@once",
    start_date=yesterday,
) as dag:

    # Task #1: Extract and Transform data
    query_data = PythonOperator(
        task_id='query_data',
        python_callable=query_data,
    )

    # Task #2: Load to bucket
    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
    )

    query_data >> load_data
