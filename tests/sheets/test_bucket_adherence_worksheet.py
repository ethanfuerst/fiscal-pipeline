import pandas as pd
import pytest
from eftoolkit.gsheets.runner import HookContext

from src.sheets.sheet_formats import (
    BUCKET_ADHERENCE_COLUMN_TITLES,
    BUCKET_ADHERENCE_COLUMN_WIDTH_MAPPING,
    BUCKET_ADHERENCE_NOTES,
    EXTRA_INCOME_ALLOCATION_COLUMN_TITLES,
    OFF_PLAN_ORANGE_FILL,
)
from src.sheets.worksheets.bucket_adherence import BucketAdherenceWorksheet
from tests.sheets.fakes import FakeDuckDB, RecordingWorksheet


@pytest.fixture
def buckets_df():
    return pd.DataFrame(
        [
            [2025, 'Needs', 100.0, 90.0, -0.10, True, True],
            [2025, 'Wants', 60.0, 70.0, 0.1667, False, True],
            [2025, 'Investments', 30.0, 35.0, 0.1667, True, True],
            [2025, 'Savings', 10.0, None, None, None, True],
        ],
        columns=[
            'year',
            'bucket',
            'target',
            'projected',
            'overage_pct',
            'on_plan_flag',
            'is_extrapolated',
        ],
    )


@pytest.fixture
def alloc_df():
    return pd.DataFrame(
        [[2025, 50.0, 10.0, 20.0, 10.0, 5.0, 5.0, True]],
        columns=['year'] + [f'c{i}' for i in range(6)] + ['is_extrapolated'],
    )


def _tables(buckets_df, alloc_df):
    return {
        'dashboards.bucket_adherence': buckets_df,
        'dashboards.extra_income_allocation': alloc_df,
    }


def test_name():
    ws = BucketAdherenceWorksheet()

    assert ws.name == 'Bucket Adherence'


def test_generate_returns_two_assets_stacked(buckets_df, alloc_df):
    db = FakeDuckDB(_tables(buckets_df, alloc_df))
    ws = BucketAdherenceWorksheet()

    assets = ws.generate({'db': db, 'sheet_name': 'Test'}, {})

    assert len(assets) == 2
    asset_a, asset_b = assets
    assert asset_a.location.cell == 'B2'
    assert asset_b.location.cell == f'B{2 + len(buckets_df) + 3}'
    assert list(asset_a.df.columns) == ['Year'] + BUCKET_ADHERENCE_COLUMN_TITLES
    assert list(asset_b.df.columns) == ['Year'] + EXTRA_INCOME_ALLOCATION_COLUMN_TITLES


def test_generate_keeps_on_plan_booleans_and_converts_nan(buckets_df, alloc_df):
    db = FakeDuckDB(_tables(buckets_df, alloc_df))
    ws = BucketAdherenceWorksheet()

    asset_a = ws.generate({'db': db, 'sheet_name': 'Test'}, {})[0]

    assert asset_a.df['On Plan'].tolist() == [True, False, True, 'N/A']
    assert asset_a.df['Projected'].tolist() == [90.0, 70.0, 35.0, 'N/A']
    assert asset_a.df['Data Type'].tolist() == ['Extrapolated'] * 4


def test_bucket_hook_formats_columns_and_stamps(buckets_df, alloc_df):
    db = FakeDuckDB(_tables(buckets_df, alloc_df))
    ws_def = BucketAdherenceWorksheet()
    asset_a = ws_def.generate({'db': db, 'sheet_name': 'Test'}, {})[0]
    fake_ws = RecordingWorksheet()
    ctx = HookContext(
        worksheet=fake_ws,
        asset=asset_a,
        worksheet_name=ws_def.name,
        runner_context={},
    )

    asset_a.post_write_hooks[0](ctx)

    format_ranges = {call[0] for call in fake_ws.format_calls}
    assert 'B2:H2' in format_ranges  # header
    assert 'D3:D6' in format_ranges  # Target currency
    assert 'F3:F6' in format_ranges  # Overage % percent
    assert fake_ws.values_writes[0][0] == 'B1'


def test_alloc_hooks_format_and_trim(buckets_df, alloc_df):
    db = FakeDuckDB(_tables(buckets_df, alloc_df))
    ws_def = BucketAdherenceWorksheet()
    asset_b = ws_def.generate({'db': db, 'sheet_name': 'Test'}, {})[1]
    fake_ws = RecordingWorksheet()
    ctx = HookContext(
        worksheet=fake_ws,
        asset=asset_b,
        worksheet_name=ws_def.name,
        runner_context={},
    )

    asset_b.post_write_hooks[0](ctx)
    asset_b.post_write_hooks[1](ctx)

    format_ranges = {call[0] for call in fake_ws.format_calls}
    assert asset_b.header_range.value in format_ranges
    assert fake_ws.resize_calls == [(asset_b.end_row + 1, 10)]


def test_get_formatting_includes_orange_off_plan_rule(buckets_df, alloc_df):
    db = FakeDuckDB(_tables(buckets_df, alloc_df))
    ws_def = BucketAdherenceWorksheet()
    assets = ws_def.generate({'db': db, 'sheet_name': 'Test'}, {})
    context = {ws_def.name: {'assets': assets}}

    formatting = ws_def.get_formatting(context)

    assert formatting is not None
    assert formatting.conditional_formats == [
        {
            'range': 'B3:H6',
            'type': 'CUSTOM_FORMULA',
            'values': ['=$G3=FALSE'],
            'format': {'backgroundColor': OFF_PLAN_ORANGE_FILL},
        }
    ]
    assert formatting.notes == BUCKET_ADHERENCE_NOTES
    assert formatting.column_widths == BUCKET_ADHERENCE_COLUMN_WIDTH_MAPPING
    assert formatting.auto_resize_columns == (2, 10)


def test_get_formatting_returns_none_without_assets():
    ws_def = BucketAdherenceWorksheet()

    assert ws_def.get_formatting({ws_def.name: {'assets': []}}) is None
