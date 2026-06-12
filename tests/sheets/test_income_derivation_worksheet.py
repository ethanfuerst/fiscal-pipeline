import pandas as pd
import pytest
from eftoolkit.gsheets.runner import HookContext

from src.sheets.sheet_formats import (
    DEFICIT_RED_FILL,
    INCOME_DERIVATION_COLUMN_TITLES,
    INCOME_DERIVATION_COLUMN_WIDTH_MAPPING,
    INCOME_DERIVATION_NOTES,
)
from src.sheets.worksheets.income_derivation import IncomeDerivationWorksheet
from tests.sheets.fakes import FakeDuckDB, RecordingWorksheet


@pytest.fixture
def income_df():
    return pd.DataFrame(
        [
            [2024] + [100.0] * 9 + [True, 0.0, -50.0, False],
            [2025] + [100.0] * 9 + [False, None, 50.0, True],
        ],
        columns=['year']
        + [f'c{i}' for i in range(9)]
        + ['match_milestone_hit', 'remaining', 'net_income', 'is_extrapolated'],
    )


def test_name():
    ws = IncomeDerivationWorksheet()

    assert ws.name == 'Income Derivation'


def test_generate_returns_single_asset_at_b2_with_renamed_columns(income_df):
    db = FakeDuckDB({'dashboards.income_derivation': income_df})
    ws = IncomeDerivationWorksheet()

    assets = ws.generate({'db': db, 'sheet_name': 'Test'}, {})

    assert len(assets) == 1
    assert assets[0].location.cell == 'B2'
    assert list(assets[0].df.columns) == ['Year'] + INCOME_DERIVATION_COLUMN_TITLES


def test_generate_converts_extrapolated_flag_and_nan(income_df):
    db = FakeDuckDB({'dashboards.income_derivation': income_df})
    ws = IncomeDerivationWorksheet()

    df = ws.generate({'db': db, 'sheet_name': 'Test'}, {})[0].df

    assert df['Data Type'].tolist() == ['Actual', 'Extrapolated']
    assert df['401k Employee Remaining'].tolist() == [0.0, 'N/A']


def test_format_and_stamp_hook_applies_format_and_timestamp(income_df):
    db = FakeDuckDB({'dashboards.income_derivation': income_df})
    ws_def = IncomeDerivationWorksheet()
    asset = ws_def.generate({'db': db, 'sheet_name': 'Test'}, {})[0]
    fake_ws = RecordingWorksheet()
    ctx = HookContext(
        worksheet=fake_ws,
        asset=asset,
        worksheet_name=ws_def.name,
        runner_context={},
    )

    asset.post_write_hooks[0](ctx)

    format_ranges = {call[0] for call in fake_ws.format_calls}
    assert 'B2:O2' in format_ranges
    assert 'C3:K' in format_ranges
    assert fake_ws.values_writes[0][0] == 'B1'
    assert fake_ws.values_writes[0][1][0][0].startswith('Last Updated: ')


def test_trim_to_data_hook_resizes_with_buffers(income_df):
    db = FakeDuckDB({'dashboards.income_derivation': income_df})
    ws_def = IncomeDerivationWorksheet()
    asset = ws_def.generate({'db': db, 'sheet_name': 'Test'}, {})[0]
    fake_ws = RecordingWorksheet()
    ctx = HookContext(
        worksheet=fake_ws,
        asset=asset,
        worksheet_name=ws_def.name,
        runner_context={},
    )

    asset.post_write_hooks[1](ctx)

    assert fake_ws.resize_calls == [(len(income_df) + 3, len(asset.df.columns) + 2)]


def test_get_formatting_includes_red_deficit_rule(income_df):
    db = FakeDuckDB({'dashboards.income_derivation': income_df})
    ws_def = IncomeDerivationWorksheet()
    assets = ws_def.generate({'db': db, 'sheet_name': 'Test'}, {})
    context = {ws_def.name: {'assets': assets}}

    formatting = ws_def.get_formatting(context)

    assert formatting is not None
    assert formatting.conditional_formats == [
        {
            'range': 'N3:N4',
            'type': 'CUSTOM_FORMULA',
            'values': ['=$N3<0'],
            'format': {'backgroundColor': DEFICIT_RED_FILL},
        }
    ]
    assert formatting.notes == INCOME_DERIVATION_NOTES
    assert formatting.column_widths == INCOME_DERIVATION_COLUMN_WIDTH_MAPPING
    assert formatting.auto_resize_columns == (2, 16)


def test_get_formatting_returns_none_without_assets():
    ws_def = IncomeDerivationWorksheet()

    assert ws_def.get_formatting({ws_def.name: {'assets': []}}) is None
