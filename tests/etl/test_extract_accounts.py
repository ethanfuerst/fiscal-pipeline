import io
import json

import pandas as pd

from src.etl.etl import extract_accounts


class _RecordingS3:
    def __init__(self):
        self.writes: list[tuple[pd.DataFrame, str]] = []

    def write_df_to_parquet(self, df: pd.DataFrame, path: str) -> None:
        # Round-trip through pyarrow to mirror the real failure mode.
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine='pyarrow', index=False)
        self.writes.append((df, path))


def test_extract_accounts_serializes_empty_dict_columns():
    budget_data = {
        'accounts': [
            {
                'id': 'a1',
                'name': 'Checking',
                'debt_interest_rates': {},
                'debt_minimum_payments': {},
            },
            {
                'id': 'a2',
                'name': 'Credit Card',
                'debt_interest_rates': {'2024-01-01': 1999},
                'debt_minimum_payments': {},
            },
        ]
    }
    s3 = _RecordingS3()

    extract_accounts(budget_data, s3)

    assert len(s3.writes) == 1
    df, path = s3.writes[0]
    assert path.endswith('/accounts.parquet')
    assert df['debt_interest_rates'].tolist() == [
        '{}',
        json.dumps({'2024-01-01': 1999}),
    ]
    assert df['debt_minimum_payments'].tolist() == ['{}', '{}']


def test_extract_accounts_passes_through_scalar_columns():
    budget_data = {
        'accounts': [
            {'id': 'a1', 'name': 'Checking', 'balance': 12345, 'closed': False},
        ]
    }
    s3 = _RecordingS3()

    extract_accounts(budget_data, s3)

    df, _ = s3.writes[0]
    assert df['balance'].tolist() == [12345]
    assert df['closed'].tolist() == [False]
