import json
import logging
import os

from dotenv import load_dotenv
from eftoolkit.gsheets.runner import DashboardRunner
from eftoolkit.utils import setup_logging

from src.sheets.worksheets.bucket_adherence import BucketAdherenceWorksheet
from src.sheets.worksheets.category_drilldown import CategoryDrilldownWorksheet
from src.sheets.worksheets.income_derivation import IncomeDerivationWorksheet
from src.sheets.worksheets.investments import InvestmentsWorksheet
from src.sheets.worksheets.overview import (
    OverviewMonthlyWorksheet,
    OverviewYearlyWorksheet,
)
from src.sheets.worksheets.runway import RunwayWorksheet
from src.utils import get_duckdb

setup_logging()
load_dotenv()

SHEET_NAMES = {
    'prod': 'Spending Dashboard',
    'dev': 'Spending Dashboard - DEV',
}


def refresh_sheets(env: str = 'prod') -> None:
    sheet_name = SHEET_NAMES[env]
    credentials = json.loads(os.getenv('GSPREAD_CREDENTIALS').replace('\n', '\\n'))
    with get_duckdb() as db:
        runner = DashboardRunner(
            config={'sheet_name': sheet_name, 'db': db},
            credentials=credentials,
            worksheets=[
                OverviewYearlyWorksheet(),
                OverviewMonthlyWorksheet(),
                IncomeDerivationWorksheet(),
                BucketAdherenceWorksheet(),
                InvestmentsWorksheet(),
                CategoryDrilldownWorksheet(),
                RunwayWorksheet(),
            ],
        )
        logging.info(f'Running dashboard refresh against {env} sheet ({sheet_name!r})')
        runner.run()
        logging.info('Sheet refresh complete')
