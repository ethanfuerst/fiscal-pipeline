import json
import logging
import os

from dotenv import load_dotenv
from eftoolkit.gsheets.runner import DashboardRunner
from eftoolkit.utils import setup_logging

from src.sheets.worksheets.overview import (
    OverviewMonthlyWorksheet,
    OverviewYearlyWorksheet,
)
from src.utils import get_duckdb

setup_logging()
load_dotenv()


def refresh_sheets() -> None:
    credentials = json.loads(os.getenv('GSPREAD_CREDENTIALS').replace('\n', '\\n'))
    with get_duckdb() as db:
        runner = DashboardRunner(
            config={'sheet_name': 'Spending Dashboard', 'db': db},
            credentials=credentials,
            worksheets=[
                OverviewYearlyWorksheet(),
                OverviewMonthlyWorksheet(),
            ],
        )
        logging.info('Running dashboard refresh')
        runner.run()
        logging.info('Sheet refresh complete')
