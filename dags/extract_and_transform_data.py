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


def query_successful_transactions(s3_client):
    """
    Run query and save file locally.\n
    Query and Transformation #1: Success rate of transactions in the last
    10 days.
    """

    query = """
    SELECT
      DATE(block_timestamp) as date,
      COUNT(id) as transactions_no,
      SUM(CASE WHEN success=TRUE THEN 1 ELSE 0 END) as successful_transactions,
      ROUND(SUM(CASE WHEN success=TRUE THEN 1 ELSE 0 END) / COUNT(id), 4) as success_rate,
    
    FROM public-data-finance.crypto_zilliqa.transactions
    GROUP BY date
    ORDER BY date DESC
    LIMIT 10;
    """

    data = run_query(s3_client, query)
    df = pd.DataFrame(data)

    # Set data type of date column
    df['date'] = pd.to_datetime(df['date'])

    fn = 'transactions_success_rates'
    save_data_as_parquet(dataframe=df, filename=fn)


def query_most_active_wallets(s3_client):
    """
    Run query and save file locally.\n
    Query and Transformation #2: Top 10 wallets that received the most
    transactions during a day.
    """

    date = ['2021', '09', '07']

    query = f"""
    SELECT to_addr, count(id) as transactions_no
    FROM `public-data-finance.crypto_zilliqa.transactions`
    WHERE DATE(block_timestamp) = DATE({date[0]},{date[1]},{date[2]}) 
    AND success=true
    GROUP BY to_addr
    ORDER BY transactions_no DESC
    LIMIT 10;
    """

    data = run_query(s3_client, query)
    df = pd.DataFrame(data)

    fn = 'most_active_wallets'
    save_data_as_parquet(dataframe=df, filename=fn)


def query_daily_avg_gas_price(s3_client):
    """
    Run query and save file locally.\n
    Query and Transformation #3: Daily average gas price from last 10 days.
    """

    query = f"""
    SELECT
      DATE(block_timestamp) as date,
      ROUND(AVG(gas_limit * gas_price)/power(10, 12), 3) as gas_per_transaction_in_billions
    FROM `public-data-finance.crypto_zilliqa.transactions`
    WHERE success=true
    GROUP BY date
    ORDER BY date DESC
    LIMIT 10;
    """

    data = run_query(s3_client, query)
    df = pd.DataFrame(data)

    # Set data type of date column
    df['date'] = pd.to_datetime(df['date'])

    fn = 'daily_average_gas_price'
    save_data_as_parquet(dataframe=df, filename=fn)


def query_data():
    # logging.basicConfig(level=logging.INFO)

    # Create file
    client = create_api_client()

    # Run queries and save file locally
    query_successful_transactions(client)
    query_most_active_wallets(client)
    query_daily_avg_gas_price(client)
