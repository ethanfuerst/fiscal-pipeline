from contextlib import contextmanager

import src.sheets.refresh_sheets as refresh_sheets_module
from src.sheets.refresh_sheets import refresh_sheets
from src.sheets.worksheets.bucket_adherence import BucketAdherenceWorksheet
from src.sheets.worksheets.category_drilldown import CategoryDrilldownWorksheet
from src.sheets.worksheets.income_derivation import IncomeDerivationWorksheet
from src.sheets.worksheets.investments import InvestmentsWorksheet
from src.sheets.worksheets.overview import (
    OverviewMonthlyWorksheet,
    OverviewYearlyWorksheet,
)
from src.sheets.worksheets.runway import RunwayWorksheet


class _RecordingRunner:
    instances: list['_RecordingRunner'] = []

    def __init__(self, config, credentials, worksheets):
        self.config = config
        self.credentials = credentials
        self.worksheets = worksheets
        self.run_called = False
        _RecordingRunner.instances.append(self)

    def run(self):
        self.run_called = True


def test_refresh_sheets_registers_all_seven_worksheets(monkeypatch):
    monkeypatch.setenv('GSPREAD_CREDENTIALS', '{"type": "service_account"}')

    @contextmanager
    def fake_duckdb():
        yield 'fake-db'

    monkeypatch.setattr(refresh_sheets_module, 'get_duckdb', fake_duckdb)
    monkeypatch.setattr(refresh_sheets_module, 'DashboardRunner', _RecordingRunner)
    _RecordingRunner.instances = []

    refresh_sheets('dev')

    runner = _RecordingRunner.instances[0]
    assert runner.run_called
    assert runner.config == {'sheet_name': 'Spending Dashboard - DEV', 'db': 'fake-db'}
    assert [type(ws) for ws in runner.worksheets] == [
        OverviewYearlyWorksheet,
        OverviewMonthlyWorksheet,
        IncomeDerivationWorksheet,
        BucketAdherenceWorksheet,
        InvestmentsWorksheet,
        CategoryDrilldownWorksheet,
        RunwayWorksheet,
    ]
