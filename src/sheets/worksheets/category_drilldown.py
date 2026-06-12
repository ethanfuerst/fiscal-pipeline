from src.sheets.sheet_formats import (
    CATEGORY_DRILLDOWN_COLUMN_TITLES,
    CATEGORY_DRILLDOWN_COLUMN_WIDTH_MAPPING,
    CATEGORY_DRILLDOWN_FORMAT,
    CATEGORY_DRILLDOWN_NOTES,
)
from src.sheets.worksheets._base import SimpleDashboardTab


class CategoryDrilldownWorksheet(SimpleDashboardTab):
    tab_name = 'Category Drilldown'
    table = 'dashboards.category_drilldown'
    column_titles = CATEGORY_DRILLDOWN_COLUMN_TITLES
    format = CATEGORY_DRILLDOWN_FORMAT
    notes = CATEGORY_DRILLDOWN_NOTES
    column_widths = CATEGORY_DRILLDOWN_COLUMN_WIDTH_MAPPING
    drop_columns = ['category_id']
