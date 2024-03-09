# ETL Pipeline - Crypto Transactions

Python ETL Pipeline that pulls crypto transactions data from a 
[Google BigQuery public dataset](https://console.cloud.google.com/marketplace/product/public-data-finance/crypto-zilliqa-dataset), 
performs summarizations and aggregations, and then stores the data
on a [LocalStack S3 Bucket](https://docs.localstack.cloud/user-guide/aws/s3/).

## Stack
- Docker Compose 
  - Apache Airflow
    - Python Operators
      - Google Cloud BigQuery API
      - AWS SDK for Python (Boto3)
  - LocalStack
    - S3 Bucket

## Features
- Google BigQuery API 
- SQL Queries
- email notification on failure

## Setup
Make sure to have [Docker Compose](https://docs.docker.com/compose/install/) 
installed and a way to open .parquet files (i.e. [Tad](https://www.tadviewer.com/)).

1. Place the credentials file provided on `./credentials/`.

2. From the project directory, build and run the app by running `docker-compose up -d`
```pycon
[+] Running 6/7
 ⠦ Network airflow_default                Created                                                                                                                17.7s 
 ✔ Container localstack                   Started                                                                                                                 1.0s 
 ✔ Container airflow-postgres-1           Healthy                                                                                                                 6.5s 
 ✔ Container airflow-airflow-init-1       Exited                                                                                                                  6.6s 
 ✔ Container airflow-airflow-scheduler-1  Started                                                                                                                17.2s 
 ✔ Container airflow-airflow-triggerer-1  Started                                                                                                                17.1s 
 ✔ Container airflow-airflow-webserver-1  Started  
```

3. Open the Airflow GUI on http://localhost:8080/ and login
with user `airflow` and password `airflow`. If DAG `zilla-transactions-DAG`
is paused, please unpause it and it should start executing automatically.

4. Once the run has been marked with the `success` tag, you can check the 
S3 Bucket on http://localhost:4566/zilliqa-transactions. It should have three
files, each specified under a `<Contents>` tag:
```pycon
# ...
<Contents>
<Key>transactions.parquet</Key>
# ...
# ...
<Contents>
<Key>receivers.parquet</Key>
```

5. Parquet files can be downloaded on:
- http://localhost:4566/zilliqa-transactions/transactions.parquet
- http://localhost:4566/zilliqa-transactions/receivers.parquet
