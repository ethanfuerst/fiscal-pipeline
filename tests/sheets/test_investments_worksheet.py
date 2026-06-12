import pandas as pd
import pytest

from src.sheets.sheet_formats import (
    INVESTMENTS_COLUMN_TITLES,
    INVESTMENTS_COLUMN_WIDTH_MAPPING,
    INVESTMENTS_NOTES,
)
from src.sheets.worksheets.investments import InvestmentsWorksheet
from tests.sheets.fakes import FakeDuckDB


@pytest.fixture
def investments_df():
    return pd.DataFrame(
        [
            [2024] + [100.0] * 14 + [False],
            [2025] + [100.0] * 13 + [None, True],
        ],
        columns=['year'] + [f'c{i}' for i in range(14)] + ['is_extrapolated'],
    )


def test_name():
    ws = InvestmentsWorksheet()

    assert ws.name == 'Investments'


def test_generate_returns_single_asset_at_b2_with_renamed_columns(investments_df):
    db = FakeDuckDB({'dashboards.investments': investments_df})
    ws = InvestmentsWorksheet()

    assets = ws.generate({'db': db, 'sheet_name': 'Test'}, {})

    assert len(assets) == 1
    assert assets[0].location.cell == 'B2'
    assert list(assets[0].df.columns) == ['Year'] + INVESTMENTS_COLUMN_TITLES


def test_generate_converts_extrapolated_flag_and_nan(investments_df):
    db = FakeDuckDB({'dashboards.investments': investments_df})
    ws = InvestmentsWorksheet()

    df = ws.generate({'db': db, 'sheet_name': 'Test'}, {})[0].df

    assert df['Data Type'].tolist() == ['Actual', 'Extrapolated']
    assert df['Emergency Top-Ups'].tolist() == [100.0, 'N/A']


def test_get_formatting_has_no_conditional_formats(investments_df):
    db = FakeDuckDB({'dashboards.investments': investments_df})
    ws_def = InvestmentsWorksheet()
    assets = ws_def.generate({'db': db, 'sheet_name': 'Test'}, {})
    context = {ws_def.name: {'assets': assets}}

    formatting = ws_def.get_formatting(context)

    assert formatting is not None
    assert formatting.conditional_formats == []
    assert formatting.notes == INVESTMENTS_NOTES
    assert formatting.column_widths == INVESTMENTS_COLUMN_WIDTH_MAPPING
    assert formatting.auto_resize_columns == (2, 18)
