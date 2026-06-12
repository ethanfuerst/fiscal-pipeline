from typing import List

from eftoolkit.gsheets.runner import WorksheetAsset

from src.sheets.sheet_formats import (
    INCOME_DERIVATION_COLUMN_TITLES,
    INCOME_DERIVATION_COLUMN_WIDTH_MAPPING,
    INCOME_DERIVATION_FORMAT,
    INCOME_DERIVATION_NOTES,
    deficit_red_rule,
)
from src.sheets.worksheets._base import SimpleDashboardTab, column_letter


class IncomeDerivationWorksheet(SimpleDashboardTab):
    tab_name = 'Income Derivation'
    table = 'dashboards.income_derivation'
    column_titles = INCOME_DERIVATION_COLUMN_TITLES
    format = INCOME_DERIVATION_FORMAT
    notes = INCOME_DERIVATION_NOTES
    column_widths = INCOME_DERIVATION_COLUMN_WIDTH_MAPPING

    def conditional_formats(self, asset: WorksheetAsset) -> List[dict]:
        net_range = asset.data_column_ranges['Net Income'].value
        net_col = column_letter(net_range)
        first_data_row = asset.start_row + 1
        return [deficit_red_rule(net_range, net_col, first_data_row)]
