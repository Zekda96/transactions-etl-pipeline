# Logging
import logging
import google.api_core.exceptions

# Data Manipulation
import json
import pandas as pd

# SQL Database
import sqlite3

# Cloud storage
from google.oauth2 import service_account
from google.cloud import bigquery


def create_api_client():
    """
    Create Google BigQuery API client.
    """

    credentials_file_path = "./credentials/gcp_bigquery_credentials.json"

    # Read credentials file
    try:
        raw_credentials = open(credentials_file_path).read()
        logging.info(f'Credentials file found.')
    except FileNotFoundError as e:
        logging.error(f'Credentials file not found. '
                      f'Please insert credentials file in \'./credentials\'')
        raise e

    # Create credentials object
    try:
        credentials = service_account.Credentials.from_service_account_info(
            json.loads(raw_credentials)
        )
        logging.info(f'Credentials in correct format.')
    except ValueError as e:
        logging.error(f'Wrong credentials format. Please use valid credentials.')
        raise e

    # Create client object and connect to API using credentials
    try:
        client = bigquery.Client(credentials=credentials)
        logging.info(f'Connection with API established.')
    except Exception as e:
        raise e

    return client


def run_query(client, query):
    """
    Run query using Google BigQuery API.
    """

    query_job = client.query(query)

    # Run Query
    try:
        rows_raw = query_job.result()
        logging.info(f'Query run successfully. Data retrieved.')
    except google.api_core.exceptions.BadRequest as e:
        logging.error(f'BadRequest: Error in query.')
        raise e
    except google.api_core.exceptions.Forbidden as e:
        logging.error('Table doesn\'t exist or user doesn\'t have access to it')
        raise e

    rows = [dict(row) for row in rows_raw]
    return rows


def create_db(table: str):
    """
    Create SQLite database and 'transactions' table.
    :param table: Name of the table inside the SQLite database.
    :return: sqlite3.Connection
    """

    db_connection = sqlite3.connect('zilliqa.db')

    cursor = db_connection.cursor()
    query = f"""

    CREATE TABLE {table} (
    id TEXT PRIMARY KEY,
    block_timestamp DATETIME,
    sender TEXT,
    to_addr TEXT,
    gas_limit NUMERIC,
    gas_price NUMERIC,
    amount NUMERIC,
    success INTEGER
    );
    """
    cursor.execute(query)
    return db_connection


def pull_data_to_db(s3_client: bigquery.Client,
                    sql_connection: sqlite3.Connection,
                    table: str
                    ):
    """
    Pull data using pagination and save to SQLite database.
    :param s3_client: bigquery.Client
    :param sql_connection: sqlite3.Connection
    :param table:
    """

    page_size = 10000
    page_num = 0
    total_data = 0

    # Load data using pagination and insert each page to SQLite database.
    # This reduces memory usage by only loading `page_size` rows at a time and
    # then data can be efficiently transformed using SQL queries.

    while True:

        query = f"""
        SELECT id, block_timestamp,
        sender, to_addr,
        gas_limit, gas_price,
        amount, success
        FROM public-data-finance.crypto_zilliqa.transactions
        WHERE DATE(block_timestamp) >= DATE(2022,09,07)
        LIMIT {page_size}
        OFFSET {page_num * page_size};
        """

        try:
            logging.info(f'Running query - page {page_num}')
            data = run_query(s3_client, query)
            total_data += len(data)
            logging.info(f'Ran successfully - returning {len(data)} rows'
                         f'\nTotal: {total_data}')

        except Exception as e:
            logging.error(f'Error in query.')
            raise e

        if not data:
            logging.info(f'Query returned empty list. Ending extraction.')
            break

        # Insert data into a SQLite database
        logging.info(f'Saving to database.')

        # Load rows data into a DataFrame
        df = pd.DataFrame(data)

        # Fix numeric type to 'amount' column
        df['amount'] = pd.to_numeric(df['amount'])

        # Save DataFrame to SQLite database
        df.to_sql(table, con=sql_connection, if_exists='append', index=False)

        page_num += 1


def extract_data():

    # Create SQLite database
    table_name = 'transactions'
    connection = create_db(table=table_name)

    # Connect to BigQuery API
    client = create_api_client()

    # Pull data using BigQuery API and save on SQLite database
    pull_data_to_db(s3_client=client,
                    sql_connection=connection,
                    table=table_name
                    )
