from src.sheets.sheet_formats import (
    INVESTMENTS_COLUMN_TITLES,
    INVESTMENTS_COLUMN_WIDTH_MAPPING,
    INVESTMENTS_FORMAT,
    INVESTMENTS_NOTES,
)
from src.sheets.worksheets._base import SimpleDashboardTab


class InvestmentsWorksheet(SimpleDashboardTab):
    tab_name = 'Investments'
    table = 'dashboards.investments'
    column_titles = INVESTMENTS_COLUMN_TITLES
    format = INVESTMENTS_FORMAT
    notes = INVESTMENTS_NOTES
    column_widths = INVESTMENTS_COLUMN_WIDTH_MAPPING
