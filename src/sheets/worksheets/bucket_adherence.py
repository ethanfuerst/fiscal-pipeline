from datetime import datetime, timezone
from typing import List

from eftoolkit.gsheets.runner import (
    CellLocation,
    HookContext,
    WorksheetAsset,
    WorksheetFormatting,
)

from src.sheets.sheet_formats import (
    BUCKET_ADHERENCE_COLUMN_FORMATS,
    BUCKET_ADHERENCE_COLUMN_TITLES,
    BUCKET_ADHERENCE_COLUMN_WIDTH_MAPPING,
    BUCKET_ADHERENCE_NOTES,
    EXTRA_INCOME_ALLOCATION_COLUMN_FORMATS,
    EXTRA_INCOME_ALLOCATION_COLUMN_TITLES,
    HEADER_FORMAT,
    off_plan_orange_rule,
)
from src.sheets.worksheets._base import (
    column_letter,
    format_asset_columns_hook,
    to_display_df,
)


class BucketAdherenceWorksheet:
    """Tab 2: 50/30/15/5 bucket adherence table plus the §8 extra-income strip."""

    tab_name = 'Bucket Adherence'

    @property
    def name(self) -> str:
        return self.tab_name

    def _format_buckets_hook(self, ctx: HookContext) -> None:
        format_asset_columns_hook(ctx, HEADER_FORMAT, BUCKET_ADHERENCE_COLUMN_FORMATS)
        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
        ctx.worksheet.write_values('B1', [[f'Last Updated: {timestamp}']])

    def _format_alloc_hook(self, ctx: HookContext) -> None:
        format_asset_columns_hook(
            ctx, HEADER_FORMAT, EXTRA_INCOME_ALLOCATION_COLUMN_FORMATS
        )

    def _trim_to_data_hook(self, ctx: HookContext) -> None:
        # Runs on the lower (§8) asset: its end_row is the sheet's last data
        # row; +1 leaves a single empty buffer row below. Width covers the
        # wider of the two assets plus left/right buffer columns.
        ctx.worksheet.resize_sheet(
            rows=ctx.asset.end_row + 1, columns=self._sheet_width
        )

    def generate(self, config: dict, context: dict) -> List[WorksheetAsset]:
        db = config['db']
        buckets = db.get_table('dashboards.bucket_adherence')
        buckets.columns = ['Year'] + BUCKET_ADHERENCE_COLUMN_TITLES
        buckets = to_display_df(buckets)
        alloc = db.get_table('dashboards.extra_income_allocation')
        alloc.columns = ['Year'] + EXTRA_INCOME_ALLOCATION_COLUMN_TITLES
        alloc = to_display_df(alloc)
        self._sheet_width = max(len(buckets.columns), len(alloc.columns)) + 2
        asset_a = WorksheetAsset(
            df=buckets,
            location=CellLocation(cell='B2'),
            post_write_hooks=[self._format_buckets_hook],
        )
        start_b = 2 + len(buckets) + 3
        asset_b = WorksheetAsset(
            df=alloc,
            location=CellLocation(cell=f'B{start_b}'),
            post_write_hooks=[self._format_alloc_hook, self._trim_to_data_hook],
        )
        return [asset_a, asset_b]

    def get_formatting(self, context: dict) -> WorksheetFormatting | None:
        assets = context[self.name]['assets']
        if not assets:
            return None
        asset_a = assets[0]
        flag_col = column_letter(asset_a.column_ranges['On Plan'].value)

        return WorksheetFormatting(
            conditional_formats=[
                off_plan_orange_rule(
                    asset_a.data_range.value, flag_col, asset_a.start_row + 1
                )
            ],
            notes=dict(BUCKET_ADHERENCE_NOTES),
            column_widths=dict(BUCKET_ADHERENCE_COLUMN_WIDTH_MAPPING),
            auto_resize_columns=(2, self._sheet_width),
        )
