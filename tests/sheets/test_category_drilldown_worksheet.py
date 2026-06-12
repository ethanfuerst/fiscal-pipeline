import pandas as pd
import pytest

from src.sheets.sheet_formats import (
    CATEGORY_DRILLDOWN_COLUMN_TITLES,
    CATEGORY_DRILLDOWN_COLUMN_WIDTH_MAPPING,
    CATEGORY_DRILLDOWN_NOTES,
)
from src.sheets.worksheets.category_drilldown import CategoryDrilldownWorksheet
from tests.sheets.fakes import FakeDuckDB


@pytest.fixture
def drilldown_df():
    return pd.DataFrame(
        [
            [2025, 'id-1', 'Needs', 'Rent', 1200.0, 1300.0, 0.65, True],
            [2025, 'id-2', 'Wants', 'Dining', 300.0, 325.0, None, True],
        ],
        columns=[
            'year',
            'category_id',
            'bucket',
            'category',
            'amount_spent',
            'projected_current_year',
            'pct_of_bucket',
            'is_extrapolated',
        ],
    )


def test_name():
    ws = CategoryDrilldownWorksheet()

    assert ws.name == 'Category Drilldown'


def test_generate_drops_category_id_and_renames_columns(drilldown_df):
    db = FakeDuckDB({'dashboards.category_drilldown': drilldown_df})
    ws = CategoryDrilldownWorksheet()

    assets = ws.generate({'db': db, 'sheet_name': 'Test'}, {})

    assert len(assets) == 1
    assert assets[0].location.cell == 'B2'
    assert list(assets[0].df.columns) == ['Year'] + CATEGORY_DRILLDOWN_COLUMN_TITLES


def test_generate_converts_extrapolated_flag_and_nan(drilldown_df):
    db = FakeDuckDB({'dashboards.category_drilldown': drilldown_df})
    ws = CategoryDrilldownWorksheet()

    df = ws.generate({'db': db, 'sheet_name': 'Test'}, {})[0].df

    assert df['Data Type'].tolist() == ['Extrapolated', 'Extrapolated']
    assert df['% of Bucket'].tolist() == [0.65, 'N/A']


def test_get_formatting_includes_notes_and_widths(drilldown_df):
    db = FakeDuckDB({'dashboards.category_drilldown': drilldown_df})
    ws_def = CategoryDrilldownWorksheet()
    assets = ws_def.generate({'db': db, 'sheet_name': 'Test'}, {})
    context = {ws_def.name: {'assets': assets}}

    formatting = ws_def.get_formatting(context)

    assert formatting is not None
    assert formatting.conditional_formats == []
    assert formatting.notes == CATEGORY_DRILLDOWN_NOTES
    assert formatting.column_widths == CATEGORY_DRILLDOWN_COLUMN_WIDTH_MAPPING
    assert formatting.auto_resize_columns == (2, 9)
