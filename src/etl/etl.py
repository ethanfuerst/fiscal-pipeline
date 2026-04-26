import json
import logging
import os
from datetime import datetime
from typing import Dict

import pandas as pd
import requests
from dotenv import load_dotenv
from eftoolkit.gsheets import Spreadsheet
from eftoolkit.s3 import S3FileSystem
from eftoolkit.utils import setup_logging

from src.utils import get_s3

setup_logging()
load_dotenv()

BUCKET_NAME = os.getenv('BUCKET_NAME')


def extract_budget_data() -> Dict:
    logging.info('Extracting budget data')

    budget_id = os.getenv('BUDGET_ID')
    bearer_token = os.getenv('BEARER_TOKEN')
    response = requests.get(
        f'https://api.ynab.com/v1/budgets/{budget_id}',
        headers={'Authorization': f'Bearer {bearer_token}'},
    )

    logging.info('Extracted budget data')

    return response.json()['data']['budget']


def extract_category_groups(budget_data: Dict, s3: S3FileSystem) -> None:
    df = pd.DataFrame(budget_data['category_groups']).reset_index(drop=True)
    s3.write_df_to_parquet(df, f's3://{BUCKET_NAME}/category-groups.parquet')


def extract_categories(budget_data: Dict, s3: S3FileSystem) -> None:
    dfs = []
    for month in budget_data['months']:
        month_date = datetime.strptime(month['month'], '%Y-%m-%d')
        monthly = pd.DataFrame(month['categories'])
        if monthly.empty:
            continue
        monthly['year'] = month_date.year
        monthly['month'] = month_date.month
        dfs.append(monthly)

    df = pd.concat(dfs).reset_index(drop=True)
    s3.write_df_to_parquet(df, f's3://{BUCKET_NAME}/monthly-categories.parquet')


def extract_transactions(budget_data: Dict, s3: S3FileSystem) -> None:
    df = pd.DataFrame(budget_data['transactions']).reset_index(drop=True)
    s3.write_df_to_parquet(df, f's3://{BUCKET_NAME}/transactions.parquet')


def extract_subtransactions(budget_data: Dict, s3: S3FileSystem) -> None:
    df = pd.DataFrame(budget_data['subtransactions']).reset_index(drop=True)
    s3.write_df_to_parquet(df, f's3://{BUCKET_NAME}/subtransactions.parquet')


def extract_accounts(budget_data: Dict, s3: S3FileSystem) -> None:
    df = pd.DataFrame(budget_data['accounts']).reset_index(drop=True)
    # YNAB returns debt_* fields as dicts keyed by date; for non-debt accounts
    # they're empty {}, which pyarrow can't write as a struct with no children.
    # Serialize any dict-valued column to a JSON string to keep the schema stable.
    for col in df.columns:
        if df[col].apply(lambda v: isinstance(v, dict)).any():
            df[col] = df[col].apply(
                lambda v: json.dumps(v) if isinstance(v, dict) else v
            )
    s3.write_df_to_parquet(df, f's3://{BUCKET_NAME}/accounts.parquet')


def load_paystubs_from_sheets(s3: S3FileSystem) -> None:
    credentials = json.loads(os.getenv('GSPREAD_CREDENTIALS').replace('\n', '\\n'))
    with Spreadsheet(credentials=credentials, spreadsheet_name='Paystubs') as ss:
        df = ss.worksheet('all_paystubs').read(dtype=str)

    df = df.reset_index(drop=True)
    s3.write_df_to_parquet(df, f's3://{BUCKET_NAME}/raw-paystubs.parquet')


def etl_ynab_data() -> None:
    s3 = get_s3()
    budget_data = extract_budget_data()

    load_paystubs_from_sheets(s3)
    extract_category_groups(budget_data, s3)
    extract_categories(budget_data, s3)
    extract_transactions(budget_data, s3)
    extract_subtransactions(budget_data, s3)
    extract_accounts(budget_data, s3)
