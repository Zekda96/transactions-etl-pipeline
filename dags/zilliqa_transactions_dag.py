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

yesterday = datetime.now() - timedelta(days=1)
print(yesterday)

with DAG(
    dag_id='zilliqa-transactions-DAG',
    default_args=default_args,
    description='ETL - Crypto transactions data to S3 bucket',
    schedule_interval="@once",
    start_date=yesterday,
) as dag:

    query_data = PythonOperator(
        task_id='query_data',
        python_callable=query_data,
    )

    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
    )

    query_data >> load_data
