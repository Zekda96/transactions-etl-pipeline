# Logging
import logging

# Data Manipulation
import pandas as pd

# SQL Database
import sqlite3


def query_transactions(sqlite_connection):
    """
    Run summarization query and save table on SQLite database.

    Metrics with large numbers have been divided by 10^6 in order to avoid
    integer overflow but not lose any meaningful information other
    processes may need down the line.

    :param sqlite_connection: Connection object to SQLite database.
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

    # Save table
    table_name = 'transactions_summary'
    df.to_sql(table_name,
              con=sqlite_connection,
              if_exists='append',
              index=False
              )

    logging.info(f'Transformed data saved on table {table_name}')


def query_receivers(sqlite_connection):
    """
    Run summarization query and save table on SQLite database.


    Metrics with large numbers have been divided by 10^6 in order to avoid
    integer overflow but not lose any meaningful information other
    processes may need down the line.

    :param sqlite_connection: Connection object to SQLite database.
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

    logging.info(f'Sending query:\n{query}')

    df = pd.read_sql_query(query, sqlite_connection)

    # Save table
    table_name = 'receivers_summary'
    df.to_sql(table_name,
              con=sqlite_connection,
              if_exists='append',
              index=False
              )

    logging.info(f'Transformed data saved on table {table_name}')


def transform_data():
    """
    Create new tables using SQL summarization and store them on SQLite database.
    """

    # Start connection
    db_connection = sqlite3.connect('zilliqa.db')

    # Summarization #1: Daily metrics since August 2022 (ending on 2022-09-07)
    query_transactions(db_connection)

    # Summarization #2: All addresses ordered by most transactions received
    query_receivers(db_connection)

    # End connection
    db_connection.close()
