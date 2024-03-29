# AWS
import boto3

# Logging
from botocore import exceptions
import logging

# Data Management
import sqlite3
import pandas as pd

# Buffer
import io


def save_file_to_s3(client, fn):
    """
    Save file to LocalStack S3 bucket.
    :param client: S3 client initialized using boto3.
    :param fn: Name of the table to pull from and the final .parquet file
    """

    bucket_name = "zilliqa-transactions"

    logging.info('Uploading file')
    # Get table from SQLite database and store it as a .parquet file on S3.
    try:
        # Connect to SQlite database
        db_connection = sqlite3.connect('zilliqa.db')

        # Pull data from table and load as DataFrame
        query = f'SELECT * FROM {fn}'
        df = pd.read_sql_query(query, db_connection)

        # Create Buffer object from DataFrame
        bytes_io = io.BytesIO()
        df.to_parquet(bytes_io, index=False)
        bytes_io.seek(0)

        # Store data on S3 bucket
        path = f"{fn}.parquet"
        client.put_object(Bucket=bucket_name, Key=path, Body=bytes_io)
        logging.info(f'Successfully connected with Localstack S3')
        logging.info(f'File {fn} uploaded to bucket: {bucket_name}')

    except exceptions.ClientError as e:
        logging.error('Upload failed.')
        raise e

    except exceptions.EndpointConnectionError as e:
        logging.error(f'Connection to LocalStack failed: could not '
                      f'connect to endpoint_url.')
        raise e


def load_data():
    """
    Uploads file to S3 bucket using S3 client object.
    """

    # Create S3 client
    s3 = boto3.client(
        service_name='s3',
        aws_access_key_id='test',
        aws_secret_access_key='test',
        endpoint_url='http://localstack:4566',
    )

    tables = ['transactions_summary',
              'receivers_summary']

    for f in tables:
        save_file_to_s3(s3, f)

