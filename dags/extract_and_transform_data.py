# Logging
import logging
import google.api_core.exceptions

# Data Manipulation
import json
import pandas as pd

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


def save_data_as_parquet(dataframe, filename):
    """
    Save pandas DataFrame as parquet.
    """

    dataframe.to_parquet(f'{filename}.parquet', compression='gzip')
    logging.info(f'Retrieved data saved on ./{filename}.parquet')


def query_transactions(s3_client):
    """
    Run query and save file locally.\n
    Query and Transformation #1: Summarization of August-2022 transactions.
    """

    # SQL Query with pagination and summarization.
    query = """
    SELECT
      DATE(block_timestamp) as date,
        COUNT(id) as num_transcations,
        SUM(CASE WHEN success=TRUE THEN 1 ELSE 0 END) as successful_transactions,
        ROUND(SUM(CASE WHEN success=TRUE THEN 1 ELSE 0 END) / COUNT(id), 4) as success_rate,
        COUNT(distinct sender) as distinct_sender,
        COUNT(distinct to_addr) as distinct_receivers,
        ROUND(AVG(gas_limit * gas_price)/power(10, 12), 3) as gas_per_transaction_in_billions,
        ROUND(AVG(amount)/power(10, 12), 3) as amount_per_transaction_in_billions,
    FROM public-data-finance.crypto_zilliqa.transactions
    WHERE DATE(block_timestamp) >= DATE(2022,08,01)
    AND DATE(block_timestamp) < DATE(2022,09,01)
    GROUP BY date
    ORDER BY date DESC
    LIMIT 31;
    """

    data = run_query(s3_client, query)
    df = pd.DataFrame(data)

    # Set data type of date column
    df['date'] = pd.to_datetime(df['date'])

    fn = 'transactions'
    save_data_as_parquet(dataframe=df, filename=fn)


def query_receivers(s3_client):
    """
    Run query and save file locally.\n
    Query and Transformation #2: Summarization of receiver addresses during
    August-2022.
    """

    # SQL Query with pagination and summarization.
    query = f"""
    SELECT
      to_addr as receiver,
      COUNT(id) as transactions_no,
      ROUND(AVG(gas_limit * gas_price)/power(10, 12), 4) as avg_gas_in_billions,
      ROUND(SUM(amount)/power(10, 12), 3) as received_in_billions,
    FROM `public-data-finance.crypto_zilliqa.transactions`
    WHERE DATE(block_timestamp) >= DATE(2021,08,01)
    AND DATE(block_timestamp) < DATE(2021,09,01)
    AND success=true
    GROUP BY receiver
    ORDER BY transactions_no DESC
    LIMIT 20;
    """

    data = run_query(s3_client, query)
    df = pd.DataFrame(data)

    fn = 'receivers'
    save_data_as_parquet(dataframe=df, filename=fn)


def query_data():
    # logging.basicConfig(level=logging.INFO)

    # Create file
    client = create_api_client()

    # Run queries and save file locally
    query_transactions(client)
    query_receivers(client)
