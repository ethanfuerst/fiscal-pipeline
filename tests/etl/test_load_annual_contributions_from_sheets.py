import io
from unittest.mock import MagicMock, patch

import pandas as pd

from src.etl.etl import load_annual_contributions_from_sheets


class _RecordingS3:
    def __init__(self):
        self.writes: list[tuple[pd.DataFrame, str]] = []

    def write_df_to_parquet(self, df: pd.DataFrame, path: str) -> None:
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine='pyarrow', index=False)
        self.writes.append((df.copy(), path))


def _sample_sheet_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                'year': '2025',
                'hsa_contribution_usd': '3300',
                'contribution_limit_401k_usd': '70000',
                'employee_contribution_limit_401k_usd': '23500',
            },
            {
                'year': '2026',
                'hsa_contribution_usd': '3375',
                'contribution_limit_401k_usd': '70000',
                'employee_contribution_limit_401k_usd': '24000',
            },
        ]
    )


def test_load_annual_contributions_from_sheets_writes_raw_parquet(monkeypatch):
    monkeypatch.setenv('GSPREAD_CREDENTIALS', '{"type": "service_account"}')

    sample_df = _sample_sheet_df()

    fake_worksheet = MagicMock()
    fake_worksheet.read.return_value = sample_df
    fake_ss_ctx = MagicMock()
    fake_ss_ctx.worksheet.return_value = fake_worksheet
    fake_spreadsheet = MagicMock()
    fake_spreadsheet.__enter__.return_value = fake_ss_ctx
    fake_spreadsheet.__exit__.return_value = False

    with patch('src.etl.etl.Spreadsheet', return_value=fake_spreadsheet) as mock_cls:
        s3 = _RecordingS3()

        load_annual_contributions_from_sheets(s3)

        assert mock_cls.call_args.kwargs['spreadsheet_name'] == 'Paystubs'
        fake_ss_ctx.worksheet.assert_called_once_with('annual_contributions')
        fake_worksheet.read.assert_called_once_with(dtype=str)

        assert len(s3.writes) == 1
        df, path = s3.writes[0]

        assert path.endswith('/raw-annual-contributions.parquet')
        assert list(df.columns) == [
            'year',
            'hsa_contribution_usd',
            'contribution_limit_401k_usd',
            'employee_contribution_limit_401k_usd',
        ]
        assert df['year'].tolist() == ['2025', '2026']


def test_load_annual_contributions_from_sheets_resets_index(monkeypatch):
    monkeypatch.setenv('GSPREAD_CREDENTIALS', '{"type": "service_account"}')

    sample_df = _sample_sheet_df()
    sample_df.index = [10, 20]

    fake_worksheet = MagicMock()
    fake_worksheet.read.return_value = sample_df
    fake_ss_ctx = MagicMock()
    fake_ss_ctx.worksheet.return_value = fake_worksheet
    fake_spreadsheet = MagicMock()
    fake_spreadsheet.__enter__.return_value = fake_ss_ctx
    fake_spreadsheet.__exit__.return_value = False

    with patch('src.etl.etl.Spreadsheet', return_value=fake_spreadsheet):
        s3 = _RecordingS3()

        load_annual_contributions_from_sheets(s3)

        df, _ = s3.writes[0]

        assert df.index.tolist() == [0, 1]
