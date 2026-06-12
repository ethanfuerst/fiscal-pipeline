import pandas as pd
import pytest
from eftoolkit.gsheets.runner import HookContext

from src.sheets.sheet_formats import (
    DEFICIT_RED_FILL,
    OFF_PLAN_ORANGE_FILL,
    RUNWAY_COLUMN_TITLES,
)
from src.sheets.worksheets.runway import RunwayWorksheet
from tests.sheets.fakes import FakeDuckDB, RecordingWorksheet


@pytest.fixture
def runway_df():
    return pd.DataFrame(
        [
            [2025, 1000.0, 300.0, 50.0, None, None, None, None, 'Healthy', True],
            [2024, 900.0, 300.0, 40.0, 1.6, 1.8, 1.4, 1.4, 'Unhealthy', False],
        ],
        columns=[
            'year',
            'liquid_cash',
            'emergency_fund_balance',
            'hsa_reimbursable_reserve',
            'runway_trailing_3mo',
            'runway_trailing_12mo',
            'runway_projected',
            'worst_runway',
            'zone',
            'is_extrapolated',
        ],
    )


@pytest.fixture
def monthly_df():
    return pd.DataFrame(
        {
            'month': ['2025-02-01', '2025-01-01', '2024-12-01'],
            'worst_runway': [2.5, None, 1.25],
        }
    )


def _tables(runway_df, monthly_df):
    return {'dashboards.runway': runway_df, 'core.monthly_runway': monthly_df}


def test_name():
    ws = RunwayWorksheet()

    assert ws.name == 'Runway'


def test_generate_returns_single_yearly_asset(runway_df, monthly_df):
    db = FakeDuckDB(_tables(runway_df, monthly_df))
    ws = RunwayWorksheet()

    assets = ws.generate({'db': db, 'sheet_name': 'Test'}, {})

    assert len(assets) == 1
    assert assets[0].location.cell == 'B2'
    assert list(assets[0].df.columns) == ['Year'] + RUNWAY_COLUMN_TITLES


def test_generate_converts_nan_to_na_in_yearly_table(runway_df, monthly_df):
    db = FakeDuckDB(_tables(runway_df, monthly_df))
    ws = RunwayWorksheet()

    df = ws.generate({'db': db, 'sheet_name': 'Test'}, {})[0].df

    assert df['Worst Runway'].tolist() == ['N/A', 1.4]
    assert df['Data Type'].tolist() == ['Extrapolated', 'Actual']


def test_sparkline_formula_inlines_values_sorted_without_gaps(runway_df, monthly_df):
    db = FakeDuckDB(_tables(runway_df, monthly_df))
    ws = RunwayWorksheet()

    formula = ws.load_sparkline_formula(db)

    assert formula == '=SPARKLINE({1.25,2.5})'


def test_sparkline_formula_is_none_when_no_monthly_data(runway_df):
    monthly = pd.DataFrame({'month': ['2025-01-01'], 'worst_runway': [None]})
    db = FakeDuckDB(_tables(runway_df, monthly))
    ws = RunwayWorksheet()

    assert ws.load_sparkline_formula(db) is None


def test_sparkline_hook_writes_label_and_inline_formula(runway_df, monthly_df):
    db = FakeDuckDB(_tables(runway_df, monthly_df))
    ws_def = RunwayWorksheet()
    asset = ws_def.generate({'db': db, 'sheet_name': 'Test'}, {})[0]
    fake_ws = RecordingWorksheet()
    ctx = HookContext(
        worksheet=fake_ws,
        asset=asset,
        worksheet_name=ws_def.name,
        runner_context={},
    )

    asset.post_write_hooks[1](ctx)

    writes = dict(fake_ws.values_writes)
    assert writes['M2'] == [['Monthly Worst Runway']]
    assert writes['M3'] == [['=SPARKLINE({1.25,2.5})']]


def test_sparkline_hook_skips_formula_when_no_monthly_data(runway_df):
    monthly = pd.DataFrame({'month': ['2025-01-01'], 'worst_runway': [None]})
    db = FakeDuckDB(_tables(runway_df, monthly))
    ws_def = RunwayWorksheet()
    asset = ws_def.generate({'db': db, 'sheet_name': 'Test'}, {})[0]
    fake_ws = RecordingWorksheet()
    ctx = HookContext(
        worksheet=fake_ws,
        asset=asset,
        worksheet_name=ws_def.name,
        runner_context={},
    )

    asset.post_write_hooks[1](ctx)

    writes = dict(fake_ws.values_writes)
    assert writes['M2'] == [['Monthly Worst Runway']]
    assert 'M3' not in writes


def test_trim_hook_resizes_past_sparkline_column(runway_df, monthly_df):
    db = FakeDuckDB(_tables(runway_df, monthly_df))
    ws_def = RunwayWorksheet()
    asset = ws_def.generate({'db': db, 'sheet_name': 'Test'}, {})[0]
    fake_ws = RecordingWorksheet()
    ctx = HookContext(
        worksheet=fake_ws,
        asset=asset,
        worksheet_name=ws_def.name,
        runner_context={},
    )

    asset.post_write_hooks[2](ctx)

    # 10 data columns + 2 buffers < 14, so the sparkline column (M) wins.
    assert fake_ws.resize_calls == [(len(runway_df) + 3, 14)]


def test_get_formatting_includes_zone_band_rules(runway_df, monthly_df):
    db = FakeDuckDB(_tables(runway_df, monthly_df))
    ws_def = RunwayWorksheet()
    assets = ws_def.generate({'db': db, 'sheet_name': 'Test'}, {})
    context = {ws_def.name: {'assets': assets}}

    formatting = ws_def.get_formatting(context)

    assert formatting is not None
    assert formatting.conditional_formats == [
        {
            'range': 'J3:J4',
            'type': 'CUSTOM_FORMULA',
            'values': ['=$J3="Unhealthy"'],
            'format': {'backgroundColor': DEFICIT_RED_FILL},
        },
        {
            'range': 'J3:J4',
            'type': 'CUSTOM_FORMULA',
            'values': ['=$J3="Watch"'],
            'format': {'backgroundColor': OFF_PLAN_ORANGE_FILL},
        },
    ]
