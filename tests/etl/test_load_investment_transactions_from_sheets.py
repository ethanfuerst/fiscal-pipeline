import io
from unittest.mock import MagicMock, patch

import pandas as pd

from src.etl.etl import load_investment_transactions_from_sheets


class _RecordingS3:
    def __init__(self):
        self.writes: list[tuple[pd.DataFrame, str]] = []

    def write_df_to_parquet(self, df: pd.DataFrame, path: str) -> None:
        # Round-trip through pyarrow to mirror the real failure mode.
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine='pyarrow', index=False)
        self.writes.append((df.copy(), path))


def _sample_sheet_df() -> pd.DataFrame:
    # Mirrors the canonical 14-column schema from process_prompt.md, all dtype=str
    # because that's how Spreadsheet.read(dtype=str) returns it.
    return pd.DataFrame(
        [
            {
                'source_file_name': 'Brokerage Statement_2024-06-30_850.PDF',
                'broker': 'schwab',
                'account_number': '5688-8850',
                'trade_date': '2024-06-03',
                'settlement_date': '2024-06-03',
                'type': 'BUY',
                'symbol': 'VTI',
                'description': 'VANGUARD TOTAL STOCK MARKET ETF',
                'quantity': '2.0000',
                'price': '288.9754',
                'amount': '-577.95',
                'fees': '',
                'currency': 'USD',
                'raw_json': '{}',
            },
            {
                'source_file_name': 'Statement10312024pdf.pdf',
                'broker': 'fidelity',
                'account_number': '246-576902',
                'trade_date': '2024-10-31',
                'settlement_date': '2024-10-31',
                'type': 'DIVIDEND',
                'symbol': 'FDRXX',
                'description': 'Dividend Received',
                'quantity': '',
                'price': '',
                'amount': '2.87',
                'fees': '',
                'currency': 'USD',
                'raw_json': (
                    '{"section":"Dividends, Interest & Other Income"'
                    ',"security_name":"FIDELITY GOVERNMENT CASH RESERVES"}'
                ),
            },
        ]
    )


def test_load_investment_transactions_from_sheets_writes_raw_parquet(monkeypatch):
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

        load_investment_transactions_from_sheets(s3)

        assert (
            mock_cls.call_args.kwargs['spreadsheet_name'] == 'Investment Transactions'
        )
        fake_ss_ctx.worksheet.assert_called_once_with('transactions')
        fake_worksheet.read.assert_called_once_with(dtype=str)

        assert len(s3.writes) == 1
        df, path = s3.writes[0]

        assert path.endswith('/raw-investment-transactions.parquet')
        assert list(df.columns) == list(sample_df.columns)
        assert len(df) == 2
        assert df['source_file_name'].tolist() == [
            'Brokerage Statement_2024-06-30_850.PDF',
            'Statement10312024pdf.pdf',
        ]


def test_load_investment_transactions_from_sheets_resets_index(monkeypatch):
    monkeypatch.setenv('GSPREAD_CREDENTIALS', '{"type": "service_account"}')

    # Simulate Spreadsheet returning a frame with a non-default index — the loader
    # must reset before writing so parquet doesn't carry along an arbitrary range.
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

        load_investment_transactions_from_sheets(s3)

        df, _ = s3.writes[0]

        assert df.index.tolist() == [0, 1]
