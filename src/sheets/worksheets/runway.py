from typing import List

from eftoolkit.gsheets.runner import CellLocation, HookContext, WorksheetAsset

from src.sheets.sheet_formats import (
    RUNWAY_COLUMN_TITLES,
    RUNWAY_COLUMN_WIDTH_MAPPING,
    RUNWAY_FORMAT,
    RUNWAY_NOTES,
    zone_band_rules,
)
from src.sheets.worksheets._base import SimpleDashboardTab, column_letter

SPARKLINE_LABEL_CELL = 'M2'
SPARKLINE_FORMULA_CELL = 'M3'


class RunwayWorksheet(SimpleDashboardTab):
    tab_name = 'Runway'
    table = 'dashboards.runway'
    column_titles = RUNWAY_COLUMN_TITLES
    format = RUNWAY_FORMAT
    notes = RUNWAY_NOTES
    column_widths = RUNWAY_COLUMN_WIDTH_MAPPING

    def load_sparkline_formula(self, db) -> str | None:
        # Documented exception: the sparkline reads core.monthly_runway
        # directly (one as-of snapshot per month, oldest first). Values are
        # inlined as an array literal so no helper range lands on the sheet;
        # months with no runway (zero burn) are dropped.
        monthly = db.get_table('core.monthly_runway').sort_values('month')
        values = [round(float(v), 4) for v in monthly['worst_runway'].dropna()]
        if not values:
            return None
        return f'=SPARKLINE({{{",".join(str(v) for v in values)}}})'

    def trim_past_sparkline_hook(self, ctx: HookContext) -> None:
        # Like the base trim, but width must cover the sparkline column
        # (M = 13) plus a right buffer.
        rows = len(ctx.asset.df) + 3
        columns = max(len(ctx.asset.df.columns) + 2, 14)
        ctx.worksheet.resize_sheet(rows=rows, columns=columns)

    def generate(self, config: dict, context: dict) -> List[WorksheetAsset]:
        db = config['db']
        formula = self.load_sparkline_formula(db)

        def sparkline_hook(ctx: HookContext) -> None:
            ctx.worksheet.write_values(SPARKLINE_LABEL_CELL, [['Monthly Worst Runway']])
            if formula is not None:
                ctx.worksheet.write_values(SPARKLINE_FORMULA_CELL, [[formula]])

        return [
            WorksheetAsset(
                df=self.load_df(db),
                location=CellLocation(cell='B2'),
                post_write_hooks=[
                    self.format_and_stamp_hook,
                    sparkline_hook,
                    self.trim_past_sparkline_hook,
                ],
            )
        ]

    def conditional_formats(self, asset: WorksheetAsset) -> List[dict]:
        zone_range = asset.data_column_ranges['Zone'].value
        zone_col = column_letter(zone_range)
        return zone_band_rules(zone_range, zone_col, asset.start_row + 1)
