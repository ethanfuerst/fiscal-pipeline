import pandas as pd
import pytest
from eftoolkit.gsheets.runner import HookContext, WorksheetAsset

from src.sheets.worksheets.adherance_waterfall import (
    WATERFALL_COLUMN_TITLES,
    AdheranceWaterfallMonthlyWorksheet,
    AdheranceWaterfallYearlyWorksheet,
)


class _FakeDuckDB:
    def __init__(self, tables):
        self._tables = tables

    def get_table(self, name, where=None):
        return self._tables[name].copy()


class _RecordingWorksheet:
    def __init__(self):
        self.format_calls: list[tuple[str, dict]] = []
        self.values_writes: list[tuple[str, list]] = []
        self.resize_calls: list[tuple[int | None, int | None]] = []

    def format_range(self, range_name, fmt):
        self.format_calls.append((range_name, fmt))

    def write_values(self, range_name, values):
        self.values_writes.append((range_name, values))

    def resize_sheet(self, rows=None, columns=None):
        self.resize_calls.append((rows, columns))


@pytest.fixture
def monthly_waterfall_df():
    return pd.DataFrame(
        [
            {
                column: '2025-02-01'
                if column == 'budget_month'
                else False
                if column == 'has_balanced_runway'
                else 0.0
                for column in WATERFALL_COLUMN_TITLES
                if column not in {'budget_year', 'runway_budget_month'}
            }
        ]
    )


@pytest.fixture
def yearly_waterfall_df():
    return pd.DataFrame(
        [
            {
                column: 2025
                if column == 'budget_year'
                else '2025-12-01'
                if column == 'runway_budget_month'
                else False
                if column == 'has_balanced_runway'
                else 0.0
                for column in WATERFALL_COLUMN_TITLES
                if column != 'budget_month'
            }
        ]
    )


def test_monthly_waterfall_worksheet_name():
    ws = AdheranceWaterfallMonthlyWorksheet()

    assert ws.name == 'Adherence Waterfall - Monthly'


def test_yearly_waterfall_worksheet_name():
    ws = AdheranceWaterfallYearlyWorksheet()

    assert ws.name == 'Adherence Waterfall - Yearly'


def test_monthly_waterfall_generate_uses_monthly_model_and_formats_month(
    monthly_waterfall_df,
):
    db = _FakeDuckDB({'dashboards.monthly_adherance_waterfall': monthly_waterfall_df})
    ws = AdheranceWaterfallMonthlyWorksheet()

    assets = ws.generate({'db': db, 'sheet_name': 'Test'}, {})

    assert len(assets) == 1
    asset = assets[0]
    assert isinstance(asset, WorksheetAsset)
    assert asset.location.cell == 'B2'
    assert asset.df['Month'].tolist() == ['2/2025']
    assert 'Plan Income' not in asset.df.columns
    assert 'Actual Rollover' not in asset.df.columns
    assert 'Runway Cash' not in asset.df.columns
    assert 'Has Balanced Runway' not in asset.df.columns
    assert 'Needs Surplus' in asset.df.columns
    assert 'Needs Target Variance' not in asset.df.columns
    assert 'Savings Saved' in asset.df.columns
    assert 'Savings Net' in asset.df.columns
    assert 'Rolling Savings Balance' in asset.df.columns
    assert 'Savings Surplus' in asset.df.columns
    assert 'Savings Target Variance' not in asset.df.columns
    assert 'Overflow Dollars' in asset.df.columns
    assert 'Buffer Target' in asset.df.columns
    assert 'Buffer Balance' in asset.df.columns
    assert 'Buffer Surplus' in asset.df.columns
    assert 'Buffer Gap' not in asset.df.columns
    assert 'Overflow to Buffer' not in asset.df.columns
    assert 'Used From Buffer' in asset.df.columns
    assert 'Uncovered Shortfall' not in asset.df.columns
    assert 'True Excess' not in asset.df.columns
    assert 'Emergency Fund Target' in asset.df.columns
    assert 'Emergency Fund Balance' in asset.df.columns
    assert 'Emergency Fund Surplus' in asset.df.columns
    assert 'Emergency Reserve Balance' not in asset.df.columns
    assert 'Emergency Reserve Surplus' not in asset.df.columns
    assert 'Reserve Surplus' in asset.df.columns
    assert 'Emergency Fund Gap' not in asset.df.columns
    assert 'HSA Value for Reimbursement' not in asset.df.columns
    assert 'Other Savings Balance' in asset.df.columns
    assert 'Monthly Cash Spend' not in asset.df.columns
    assert 'Avg Monthly Cash Spend - Prior 3 Months' not in asset.df.columns
    assert 'Cash Available to Invest' not in asset.df.columns
    assert 'Needs Gap Covered' not in asset.df.columns


def test_yearly_waterfall_generate_uses_yearly_model_and_hides_runway_month(
    yearly_waterfall_df,
):
    db = _FakeDuckDB({'dashboards.yearly_adherance_waterfall': yearly_waterfall_df})
    ws = AdheranceWaterfallYearlyWorksheet()

    assets = ws.generate({'db': db, 'sheet_name': 'Test'}, {})

    assert assets[0].df['Year'].tolist() == [2025]
    assert 'Runway Month' not in assets[0].df.columns
    assert 'Buffer Balance' in assets[0].df.columns
    assert 'Buffer Surplus' in assets[0].df.columns
    assert 'Used From Buffer' in assets[0].df.columns
    assert 'Uncovered Shortfall' not in assets[0].df.columns
    assert 'Emergency Fund Surplus' in assets[0].df.columns
    assert 'Emergency Reserve Balance' not in assets[0].df.columns
    assert 'Emergency Reserve Surplus' not in assets[0].df.columns
    assert 'Reserve Surplus' in assets[0].df.columns
    assert 'Emergency Fund Gap' not in assets[0].df.columns
    assert 'HSA Value for Reimbursement' not in assets[0].df.columns
    assert 'Monthly Cash Spend' not in assets[0].df.columns


def test_waterfall_post_write_hook_applies_dynamic_formatting_and_timestamp(
    monthly_waterfall_df,
):
    db = _FakeDuckDB({'dashboards.monthly_adherance_waterfall': monthly_waterfall_df})
    ws_def = AdheranceWaterfallMonthlyWorksheet()
    asset = ws_def.generate({'db': db, 'sheet_name': 'Test'}, {})[0]
    fake_ws = _RecordingWorksheet()
    ctx = HookContext(
        worksheet=fake_ws,
        asset=asset,
        worksheet_name=ws_def.name,
        runner_context={},
    )

    asset.post_write_hooks[0](ctx)

    format_ranges = {call[0] for call in fake_ws.format_calls}
    assert 'B2:AB2' in format_ranges
    assert 'C3:C' in format_ranges
    assert 'S3:S' in format_ranges
    assert fake_ws.values_writes[0][0] == 'B1'
    assert fake_ws.values_writes[0][1][0][0].startswith('Last Updated: ')


def test_waterfall_trim_to_data_hook_resizes_to_one_row_and_column_buffer(
    monthly_waterfall_df,
):
    db = _FakeDuckDB({'dashboards.monthly_adherance_waterfall': monthly_waterfall_df})
    ws_def = AdheranceWaterfallMonthlyWorksheet()
    asset = ws_def.generate({'db': db, 'sheet_name': 'Test'}, {})[0]
    fake_ws = _RecordingWorksheet()
    ctx = HookContext(
        worksheet=fake_ws,
        asset=asset,
        worksheet_name=ws_def.name,
        runner_context={},
    )

    asset.post_write_hooks[1](ctx)

    assert fake_ws.resize_calls == [
        (len(monthly_waterfall_df) + 3, len(asset.df.columns) + 2)
    ]


def test_waterfall_get_formatting_includes_notes_widths_borders_resize(
    yearly_waterfall_df,
):
    db = _FakeDuckDB({'dashboards.yearly_adherance_waterfall': yearly_waterfall_df})
    ws_def = AdheranceWaterfallYearlyWorksheet()
    assets = ws_def.generate({'db': db, 'sheet_name': 'Test'}, {})
    context = {
        ws_def.name: {
            'assets': assets,
            'total_rows': len(yearly_waterfall_df),
            'asset_count': 1,
        }
    }

    formatting = ws_def.get_formatting(context)

    assert formatting is not None
    assert formatting.auto_resize_columns == (2, 29)
    assert formatting.column_widths['A'] == 21
    assert formatting.column_widths['AC'] == 21
    assert 'B2:AB2' in formatting.borders
    assert 'AB2' in formatting.borders


def test_waterfall_get_formatting_adds_note_to_every_header(yearly_waterfall_df):
    db = _FakeDuckDB({'dashboards.yearly_adherance_waterfall': yearly_waterfall_df})
    ws_def = AdheranceWaterfallYearlyWorksheet()
    assets = ws_def.generate({'db': db, 'sheet_name': 'Test'}, {})
    context = {
        ws_def.name: {
            'assets': assets,
            'total_rows': len(yearly_waterfall_df),
            'asset_count': 1,
        }
    }

    formatting = ws_def.get_formatting(context)

    assert formatting is not None
    assert len(formatting.notes) == len(assets[0].df.columns)
