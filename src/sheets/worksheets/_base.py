from datetime import datetime, timezone
from typing import Any, Dict, List

from eftoolkit.gsheets.runner import (
    CellLocation,
    HookContext,
    WorksheetAsset,
    WorksheetFormatting,
)
from pandas import DataFrame

EXTRAPOLATED_LABEL_COLUMN = 'Data Type'


def to_display_df(df: DataFrame) -> DataFrame:
    """Convert warehouse values for display: extrapolated flag -> label, NaN -> 'N/A'."""
    if EXTRAPOLATED_LABEL_COLUMN in df.columns:
        df[EXTRAPOLATED_LABEL_COLUMN] = df[EXTRAPOLATED_LABEL_COLUMN].map(
            {True: 'Extrapolated', False: 'Actual'}
        )
    df = df.astype(object)
    return df.where(df.notna(), 'N/A')


def column_letter(range_value: str) -> str:
    """Extract the column letter from an A1-notation range (e.g. 'N3:N12' -> 'N')."""
    return range_value.split(':')[0].rstrip('0123456789')


def format_asset_columns_hook(
    ctx: HookContext, header_format: dict, column_formats: dict
) -> None:
    """Format an asset's header row and per-column data ranges from its computed ranges."""
    ctx.worksheet.format_range(ctx.asset.header_range.value, header_format)
    data_column_ranges = ctx.asset.data_column_ranges
    for title, spec in column_formats.items():
        ctx.worksheet.format_range(data_column_ranges[title].value, spec)


class SimpleDashboardTab:
    """Base for single-asset dashboard tabs: read one dashboards.* table, rename, write at B2."""

    tab_name: str
    table: str
    column_titles: List[str]
    period_label: str = 'Year'
    format: Dict[str, Dict[str, Any]] = {}
    notes: Dict[str, str] = {}
    column_widths: Dict[str, int] = {}
    drop_columns: List[str] = []

    @property
    def name(self) -> str:
        return self.tab_name

    def load_df(self, db) -> DataFrame:
        df = db.get_table(self.table)
        if self.drop_columns:
            df = df.drop(columns=self.drop_columns)
        df.columns = [self.period_label] + self.column_titles
        return to_display_df(df)

    def format_and_stamp_hook(self, ctx: HookContext) -> None:
        for range_name, spec in self.format.items():
            ctx.worksheet.format_range(range_name, spec)
        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
        ctx.worksheet.write_values('B1', [[f'Last Updated: {timestamp}']])

    def trim_to_data_hook(self, ctx: HookContext) -> None:
        # Header writes at B2, so the last data row is row 2 + len(df).
        # +1 leaves a single empty buffer row below; column A is the left
        # buffer and one extra column is the right buffer.
        num_rows = len(ctx.asset.df)
        num_cols = len(ctx.asset.df.columns)
        ctx.worksheet.resize_sheet(rows=num_rows + 3, columns=num_cols + 2)

    def generate(self, config: dict, context: dict) -> List[WorksheetAsset]:
        df = self.load_df(config['db'])
        return [
            WorksheetAsset(
                df=df,
                location=CellLocation(cell='B2'),
                post_write_hooks=[self.format_and_stamp_hook, self.trim_to_data_hook],
            )
        ]

    def conditional_formats(self, asset: WorksheetAsset) -> List[dict]:
        return []

    def get_formatting(self, context: dict) -> WorksheetFormatting | None:
        assets = context[self.name]['assets']
        if not assets:
            return None
        sheet_width = len(assets[0].df.columns) + 2

        return WorksheetFormatting(
            conditional_formats=self.conditional_formats(assets[0]),
            notes=dict(self.notes),
            column_widths=dict(self.column_widths),
            auto_resize_columns=(2, sheet_width),
        )
