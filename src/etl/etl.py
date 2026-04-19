import json
import logging
import os
from datetime import datetime
from typing import Dict

import duckdb
import pandas as pd
import requests
from dotenv import load_dotenv
from gspread import service_account_from_dict

from src.utils.db_connection import DuckDBConnection
from src.utils.s3_utils import load_df_to_s3_table

load_dotenv()


def extract_budget_data() -> Dict:
    logging.info('Extracting budget data')

    budget_id = os.getenv('BUDGET_ID')
    url = f'https://api.ynab.com/v1/budgets/{budget_id}'

    bearer_token = os.getenv('BEARER_TOKEN')
    headers = {
        'Authorization': f'Bearer {bearer_token}',
    }

    response = requests.get(url, headers=headers)
    budget_data = response.json()['data']['budget']

    logging.info('Extracted budget data')

    return budget_data


def extract_category_groups(
    budget_data: Dict, duckdb_con: duckdb.DuckDBPyConnection
) -> None:
    category_groups = pd.DataFrame(budget_data['category_groups'])

    rows_loaded = load_df_to_s3_table(
        duckdb_con=duckdb_con,
        df=category_groups.reset_index(drop=True),
        s3_key='category-groups',
        bucket_name=os.getenv('BUCKET_NAME'),
    )

    logging.info(f'Loaded {rows_loaded} rows')


def extract_categories(
    budget_data: Dict, duckdb_con: duckdb.DuckDBPyConnection
) -> None:
    dfs = []

    for month in budget_data['months']:
        month_date = month['month']
        monthly_categories = pd.DataFrame(month['categories'])

        month_date = datetime.strptime(month_date, '%Y-%m-%d')
        monthly_categories['year'] = month_date.year
        monthly_categories['month'] = month_date.month

        if not monthly_categories.empty:
            dfs.append(monthly_categories)

    df = pd.concat(dfs)

    rows_loaded = load_df_to_s3_table(
        duckdb_con=duckdb_con,
        df=df.reset_index(drop=True),
        s3_key='monthly-categories',
        bucket_name=os.getenv('BUCKET_NAME'),
    )

    logging.info(f'Loaded {rows_loaded} rows')


def extract_transactions(
    budget_data: Dict, duckdb_con: duckdb.DuckDBPyConnection
) -> None:
    transactions = pd.DataFrame(budget_data['transactions'])

    rows_loaded = load_df_to_s3_table(
        duckdb_con=duckdb_con,
        df=transactions.reset_index(drop=True),
        s3_key='transactions',
        bucket_name=os.getenv('BUCKET_NAME'),
    )

    logging.info(f'Loaded {rows_loaded} rows')


def extract_subtransactions(
    budget_data: Dict, duckdb_con: duckdb.DuckDBPyConnection
) -> None:
    subtransactions = pd.DataFrame(budget_data['subtransactions'])

    rows_loaded = load_df_to_s3_table(
        duckdb_con=duckdb_con,
        df=subtransactions,
        s3_key='subtransactions',
        bucket_name=os.getenv('BUCKET_NAME'),
    )

    logging.info(f'Loaded {rows_loaded} rows')


def load_paystubs_from_sheets(duckdb_con: duckdb.DuckDBPyConnection) -> None:
    credentials_dict = json.loads(os.getenv('GSPREAD_CREDENTIALS').replace('\n', '\\n'))
    gc = service_account_from_dict(credentials_dict)
    worksheet = gc.open('Paystubs').worksheet('all_paystubs')

    raw = worksheet.get_all_values()
    df = pd.DataFrame(data=raw[1:], columns=raw[0]).astype(str)

    logging.info('Loading paystubs to S3 bucket')

    rows_loaded = load_df_to_s3_table(
        duckdb_con=duckdb_con,
        df=df,
        s3_key='raw-paystubs',
        bucket_name=os.getenv('BUCKET_NAME'),
    )

    logging.info(f'Loaded {rows_loaded} rows to S3 bucket for raw-paystubs')


def extract_accounts(budget_data: Dict, duckdb_con: duckdb.DuckDBPyConnection) -> None:
    accounts = pd.DataFrame(budget_data['accounts'])

    rows_loaded = load_df_to_s3_table(
        duckdb_con=duckdb_con,
        df=accounts,
        s3_key='accounts',
        bucket_name=os.getenv('BUCKET_NAME'),
    )

    logging.info(f'Loaded {rows_loaded} rows to S3 bucket for accounts')


etl_functions = {
    'category-groups': extract_category_groups,
    'monthly-categories': extract_categories,
    'transactions': extract_transactions,
    'subtransactions': extract_subtransactions,
    'accounts': extract_accounts,
}


def etl_ynab_data():
    budget_data = extract_budget_data()

    duckdb_con = DuckDBConnection().get_connection()

    load_paystubs_from_sheets(duckdb_con)

    logging.info('Extracting data from endpoints')
    for endpoint, function in etl_functions.items():
        logging.info(f'Extracting {endpoint} data')
        function(budget_data, duckdb_con)
        logging.info(f'Extracted {endpoint} data')
