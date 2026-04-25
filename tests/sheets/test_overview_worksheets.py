import pandas as pd
import pytest
from eftoolkit.gsheets.runner import HookContext, WorksheetAsset

from src.sheets.sheet_formats import (
    OVERVIEW_COLUMN_TITLES,
    OVERVIEW_COLUMN_WIDTH_MAPPING,
    OVERVIEW_NOTES,
)
from src.sheets.worksheets.overview import (
    OverviewMonthlyWorksheet,
    OverviewYearlyWorksheet,
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

    def format_range(self, range_name, fmt):
        self.format_calls.append((range_name, fmt))

    def write_values(self, range_name, values):
        self.values_writes.append((range_name, values))


@pytest.fixture
def yearly_df():
    return pd.DataFrame(
        [[2024] + [0.0] * 23, [2025] + [0.0] * 23],
        columns=['y'] + [f'c{i}' for i in range(23)],
    )


@pytest.fixture
def monthly_df():
    return pd.DataFrame(
        [['2025-01-01'] + [0.0] * 23, ['2025-02-01'] + [0.0] * 23],
        columns=['m'] + [f'c{i}' for i in range(23)],
    )


def test_yearly_worksheet_name():
    ws = OverviewYearlyWorksheet()

    assert ws.name == 'Overview - Yearly'


def test_monthly_worksheet_name():
    ws = OverviewMonthlyWorksheet()

    assert ws.name == 'Overview - Monthly'


def test_yearly_generate_returns_single_asset_at_b2(yearly_df):
    db = _FakeDuckDB({'dashboards.yearly_level': yearly_df})
    ws = OverviewYearlyWorksheet()

    assets = ws.generate({'db': db, 'sheet_name': 'Test'}, {})

    assert len(assets) == 1
    asset = assets[0]
    assert isinstance(asset, WorksheetAsset)
    assert asset.location.cell == 'B2'
    assert list(asset.df.columns)[0] == 'Year'
    assert list(asset.df.columns)[1:] == OVERVIEW_COLUMN_TITLES


def test_monthly_generate_formats_dates_as_m_yyyy(monthly_df):
    db = _FakeDuckDB({'dashboards.monthly_level': monthly_df})
    ws = OverviewMonthlyWorksheet()

    assets = ws.generate({'db': db, 'sheet_name': 'Test'}, {})

    assert assets[0].df['Month'].tolist() == ['1/2025', '2/2025']


def test_post_write_hook_applies_multi_range_format_and_timestamp(yearly_df):
    db = _FakeDuckDB({'dashboards.yearly_level': yearly_df})
    ws_def = OverviewYearlyWorksheet()
    assets = ws_def.generate({'db': db, 'sheet_name': 'Test'}, {})
    asset = assets[0]
    fake_ws = _RecordingWorksheet()
    ctx = HookContext(
        worksheet=fake_ws,
        asset=asset,
        worksheet_name=ws_def.name,
        runner_context={},
    )

    asset.post_write_hooks[0](ctx)

    format_ranges = {call[0] for call in fake_ws.format_calls}
    assert 'B2:Y2' in format_ranges
    assert 'C3:Y' in format_ranges
    assert len(fake_ws.values_writes) == 1
    last_updated_range, last_updated_values = fake_ws.values_writes[0]
    assert last_updated_range == 'B1'
    assert last_updated_values[0][0].startswith('Last Updated: ')


def test_get_formatting_includes_notes_widths_borders_resize(yearly_df):
    db = _FakeDuckDB({'dashboards.yearly_level': yearly_df})
    ws_def = OverviewYearlyWorksheet()
    assets = ws_def.generate({'db': db, 'sheet_name': 'Test'}, {})
    context = {
        ws_def.name: {
            'assets': assets,
            'total_rows': len(yearly_df),
            'asset_count': 1,
        }
    }

    formatting = ws_def.get_formatting(context)

    assert formatting is not None
    assert formatting.notes == OVERVIEW_NOTES
    assert formatting.column_widths == OVERVIEW_COLUMN_WIDTH_MAPPING
    assert formatting.auto_resize_columns == (2, 26)
    assert 'B2' in formatting.borders
    assert 'Y2' in formatting.borders
