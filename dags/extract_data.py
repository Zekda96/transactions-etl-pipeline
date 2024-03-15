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

    :return: BigQuery Client used to send queries.
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


def run_query(client: bigquery.Client,
              query: str
              ):
    """
    Run query using Google BigQuery API.

    :param client: BigQuery Client object.
    :param query: Query to execute.
    :return: rows: List of dicts representing rows from the table.
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
    :return: Connection object to SQLite database.
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

    cursor.close()

    return db_connection


def pull_data_to_db(google_client: bigquery.Client,
                    sql_connection: sqlite3.Connection,
                    table: str,
                    page_size=10000,
                    ):
    """
    Pull data using pagination and save to SQLite database.
    This reduces memory usage by only loading `page_size` rows at a time and
    afterwards data can be efficiently transformed using SQL queries.

    :param google_client: BigQuery client to send queries.
    :param sql_connection: Connection object to SQLite Database.
    :param table: Name of table inside SQLite database.
    :param page_size: Number of rows to pull at a time from BigQuery.
    """

    query = f"""
    SELECT id, block_timestamp,
    sender, to_addr,
    gas_limit, gas_price,
    amount, success,
    FROM public-data-finance.crypto_zilliqa.transactions
    WHERE DATE(block_timestamp) >= DATE(2022,08,01)
    """

    try:
        # Run query
        logging.info(f'Running query:\n{query}')
        job = google_client.query(query)
        result = job.result(page_size=page_size)

        # Buffer `page_size` rows at a time and insert in table
        logging.info('Start iterable')
        total_data = 0
        for df in result.to_dataframe_iterable():

            # DataFrame will have at most `page_size` rows
            total_data += len(df)
            logging.info(f'Ran successfully - returning {len(df)} rows'
                         f'\nTotal: {total_data}')

            # Set datatype to numeric
            df['amount'] = pd.to_numeric(df['amount'])

            # Insert DataFrame in a SQLite database
            logging.info(f'Saving to database.')
            df.to_sql(table, con=sql_connection, if_exists='append',
                      index=False)

    except Exception as e:
        logging.error(f'Error in extraction.')
        raise e


def extract_data():

    # Create SQLite database
    table_name = 'transactions'
    connection = create_db(table=table_name)

    # Connect to BigQuery API
    client = create_api_client()

    # Pull data using BigQuery API and save on SQLite database
    pull_data_to_db(google_client=client,
                    sql_connection=connection,
                    table=table_name,
                    page_size=50000,
                    )

    connection.close()
