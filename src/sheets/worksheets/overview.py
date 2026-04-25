from datetime import datetime, timezone
from typing import Any, Dict, List

from eftoolkit.gsheets.runner import (
    CellLocation,
    HookContext,
    WorksheetAsset,
    WorksheetFormatting,
)
from pandas import DataFrame, to_datetime

from src.sheets.sheet_formats import (
    OVERVIEW_COLUMN_TITLES,
    OVERVIEW_COLUMN_WIDTH_MAPPING,
    OVERVIEW_MONTHLY_FORMAT,
    OVERVIEW_NOTES,
    OVERVIEW_YEARLY_FORMAT,
)


class OverviewWorksheetBase:
    grain: str

    @property
    def name(self) -> str:
        return f'Overview - {self.grain.capitalize()}'

    @property
    def column_label(self) -> str:
        return self.grain.capitalize().replace('ly', '')

    @property
    def format(self) -> Dict[str, Dict[str, Any]]:
        return (
            OVERVIEW_MONTHLY_FORMAT
            if self.grain == 'monthly'
            else OVERVIEW_YEARLY_FORMAT
        )

    def load_df(self, db) -> DataFrame:
        df = db.get_table(f'dashboards.{self.grain}_level')
        df.columns = [self.column_label] + OVERVIEW_COLUMN_TITLES
        if self.grain == 'monthly':
            df[self.column_label] = to_datetime(df[self.column_label]).dt.strftime(
                '%-m/%Y'
            )
        return df

    def format_and_stamp_hook(self, ctx: HookContext) -> None:
        for range_name, spec in self.format.items():
            ctx.worksheet.format_range(range_name, spec)
        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
        ctx.worksheet.write_values('B1', [[f'Last Updated: {timestamp}']])

    def generate(self, config: dict, context: dict) -> List[WorksheetAsset]:
        df = self.load_df(config['db'])
        return [
            WorksheetAsset(
                df=df,
                location=CellLocation(cell='B2'),
                post_write_hooks=[self.format_and_stamp_hook],
            )
        ]

    def get_formatting(self, context: dict) -> WorksheetFormatting | None:
        assets = context[self.name]['assets']
        if not assets:
            return None
        num_rows = assets[0].num_rows
        sheet_height = num_rows + 3
        sheet_width = len(assets[0].df.columns) + 2

        return WorksheetFormatting(
            notes=dict(OVERVIEW_NOTES),
            column_widths=dict(OVERVIEW_COLUMN_WIDTH_MAPPING),
            borders=overview_borders(sheet_height),
            auto_resize_columns=(2, sheet_width),
        )


def overview_borders(sheet_height: int) -> Dict[str, Dict[str, Any]]:
    """Build a {range: border_spec} dict for the overview dashboard frame."""
    borders: Dict[str, Dict[str, Any]] = {}

    if sheet_height > 4:
        borders[f'B3:B{sheet_height - 2}'] = {'left': {'style': 'SOLID'}}
        borders[f'Y3:Y{sheet_height - 2}'] = {'right': {'style': 'SOLID'}}

    borders['C2:X2'] = {
        'top': {'style': 'SOLID'},
        'bottom': {'style': 'SOLID'},
    }
    borders[f'C{sheet_height - 1}:X{sheet_height - 1}'] = {'bottom': {'style': 'SOLID'}}

    borders['B2'] = {
        'left': {'style': 'SOLID'},
        'top': {'style': 'SOLID'},
        'bottom': {'style': 'SOLID'},
    }
    borders['Y2'] = {
        'right': {'style': 'SOLID'},
        'top': {'style': 'SOLID'},
        'bottom': {'style': 'SOLID'},
    }
    borders[f'B{sheet_height - 1}'] = {
        'left': {'style': 'SOLID'},
        'bottom': {'style': 'SOLID'},
    }
    borders[f'Y{sheet_height - 1}'] = {
        'right': {'style': 'SOLID'},
        'bottom': {'style': 'SOLID'},
    }

    columns_to_format = ['C', 'D', 'F', 'K', 'L', 'P', 'T', 'W', 'X', 'Y']
    for col in columns_to_format:
        is_column_y = col == 'Y'
        sides_middle: Dict[str, Any] = {'left': {'style': 'SOLID'}}
        sides_top: Dict[str, Any] = {
            'left': {'style': 'SOLID'},
            'top': {'style': 'SOLID'},
        }
        sides_bottom: Dict[str, Any] = {
            'left': {'style': 'SOLID'},
            'bottom': {'style': 'SOLID'},
        }
        if is_column_y:
            sides_middle['right'] = {'style': 'SOLID'}
            sides_top['right'] = {'style': 'SOLID'}
            sides_bottom['right'] = {'style': 'SOLID'}

        if sheet_height > 5:
            borders[f'{col}4:{col}{sheet_height - 2}'] = sides_middle

        borders[f'{col}3'] = sides_top
        borders[f'{col}{sheet_height - 1}'] = sides_bottom

    return borders


class OverviewYearlyWorksheet(OverviewWorksheetBase):
    grain = 'yearly'


class OverviewMonthlyWorksheet(OverviewWorksheetBase):
    grain = 'monthly'
