# Logging
import logging
import google.api_core.exceptions

# Data Manipulation
import json
import pandas as pd

# SQL Database
import sqlite3


def create_tables(sqlite_connection):

    cursor = sqlite_connection.cursor()

    query = f"""
    CREATE TABLE transactions_summary (
    date DATETIME PRIMARY KEY,
    num_transactions NUMERIC,
    successful_transactions NUMERIC,
    success_rate NUMERIC,
    distinct_sender NUMERIC,
    distinct_receiver NUMERIC,
    avg_gas_price_in_millions NUMERIC,
    avg_amount_in_millions NUMERIC
    );
    """
    cursor.execute(query)

    query = f"""
    CREATE TABLE receivers_summary (
    receiver TEXT PRIMARY KEY,
    num_transactions NUMERIC,
    avg_gas_in_millions NUMERIC,
    received_in_millions NUMERIC
    );
    """
    cursor.execute(query)

    cursor.close()


def query_transactions(sqlite_connection):
    """
    Run query and save file locally.\n
    Query and Transformation #1: Summarization of daily transactions.

    Metrics with large numbers have been reduced by 10^6 in order to avoid
    integer overflow but not lose any meaningful information other
    processes may need down the line.
    """

    # SQL Query for summarization.
    query = """
    SELECT
        DATE(block_timestamp) as date,
        COUNT(id) as num_transactions,
        SUM(success) as successful_transactions,
        (SUM(cast(success as real)) / COUNT(id)) as success_rate,
        COUNT(distinct sender) as distinct_sender,
        COUNT(distinct to_addr) as distinct_receivers,
        AVG(gas_limit * gas_price)/POWER(10, 6) as avg_gas_price_in_millions,
        AVG(amount)/POWER(10, 6) as avg_amount_in_millions
    FROM transactions
    GROUP BY date
    ORDER BY date DESC
    """
    logging.info(f'Sending query:\n{query}')

    df = pd.read_sql_query(query, sqlite_connection)

    # Save file
    table_name = 'transactions_summary'
    df.to_sql(table_name, con=sqlite_connection, if_exists='append', index=False)
    # df.to_parquet(f'{fn}.parquet', compression='gzip')
    # logging.info(f'Transformed data saved on ./{fn}.parquet')


def query_receivers(sqlite_connection):
    """
    Run query and save file locally.\n
    Query and Transformation #2: Summarization of receiver addresses during
    August-2022.

    Metrics with large numbers have been reduced by 10^6 in order to avoid
    integer overflow but not lose any meaningful information other
    processes may need down the line.
    """

    # SQL Query for summarization.

    query = f"""
    SELECT
        to_addr as receiver,
        COUNT(id) as num_transactions,
        AVG(gas_limit * gas_price/POWER(10, 6)) as avg_gas_in_millions,
        SUM(amount/POWER(10, 6)) as received_in_millions
    FROM transactions
    GROUP BY receiver
    ORDER BY num_transactions DESC
    """

    df = pd.read_sql_query(query, sqlite_connection)
    # print(df.head())

    # Save file
    table_name = 'receivers_summary'
    df.to_sql(table_name, con=sqlite_connection, if_exists='append',
              index=False)

    # fn = 'receivers'
    # df.to_parquet(f'{fn}.parquet', compression='gzip')
    # logging.info(f'Transformed data saved on ./{fn}.parquet')


def transform_data():
    db_connection = sqlite3.connect('zilliqa.db')

    # create_tables(db_connection)

    query_transactions(db_connection)
    query_receivers(db_connection)

    db_connection.close()
