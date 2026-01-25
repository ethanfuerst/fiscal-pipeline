import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict

from dotenv import load_dotenv
from gspread import Spreadsheet, Worksheet, service_account_from_dict
from gspread.exceptions import WorksheetNotFound
from pandas import DataFrame, to_datetime

from src.sheets.batcher import SheetBatcher
from src.sheets.sheet_formats import (
    OVERVIEW_COLUMN_TITLES,
    OVERVIEW_COLUMN_WIDTH_MAPPING,
    OVERVIEW_MONTHLY_FORMAT,
    OVERVIEW_NOTES,
    OVERVIEW_YEARLY_FORMAT,
    YEARLY_CATEGORIES_COLUMN_WIDTH_MAPPING,
    YEARLY_CATEGORIES_FORMAT,
    YEARLY_CATEGORIES_NOTES,
)
from src.sheets.utils import get_df_from_table
from src.utils.logging_config import setup_logging

setup_logging()
load_dotenv()


def delete_worksheet_if_exists(sh: Spreadsheet, title: str) -> None:
    """Delete a worksheet if it exists, ignoring errors."""
    try:
        ws = sh.worksheet(title)
        sh.del_worksheet(ws)
        logging.info(f'Deleted existing worksheet: {title}')
    except WorksheetNotFound:
        pass
    except Exception as e:
        logging.warning(f'Error deleting worksheet {title}: {e}')


def create_worksheet(
    sh: Spreadsheet,
    title: str,
    rows: int,
    cols: int,
    batcher: SheetBatcher,
) -> Worksheet:
    """Create a new worksheet, deleting any existing one with the same name."""
    logging.info(f'Creating worksheet: {title}')
    delete_worksheet_if_exists(sh, title)

    # Synchronous - need the object immediately for subsequent queue operations
    worksheet = sh.add_worksheet(title=title, rows=rows, cols=cols)
    batcher.register_worksheet(worksheet)

    logging.info(f'Created worksheet: {title}')
    return worksheet


def queue_last_updated_cell(
    batcher: SheetBatcher,
    worksheet: Worksheet,
    cell: str = 'B1',
) -> None:
    """Queue a 'Last Updated' cell update."""
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    batcher.queue_values(f'{worksheet.title}!{cell}', [[f'Last Updated: {timestamp}']])


def queue_df_to_sheet(
    batcher: SheetBatcher,
    df: DataFrame,
    worksheet: Worksheet,
    location: str,
    format_dict: Dict[str, Any] = None,
) -> None:
    """Queue a DataFrame to be written to a sheet."""
    values = [df.columns.values.tolist()] + df.values.tolist()
    batcher.queue_values(f'{worksheet.title}!{location}', values)

    logging.info(
        f'Queued {df.shape[0]} rows and {df.shape[1]} columns for {worksheet.title}!{location}'
    )

    if format_dict:
        queue_format_dict(batcher, worksheet, format_dict)


def queue_format_dict(
    batcher: SheetBatcher,
    worksheet: Worksheet,
    format_dict: Dict[str, Any],
) -> None:
    """Queue formatting from a format dictionary."""
    for range_name, format_spec in format_dict.items():
        batcher.queue_format(range_name, format_spec, worksheet)
        logging.debug(f'Queued format for {worksheet.title}!{range_name}')


def queue_column_widths(
    batcher: SheetBatcher,
    worksheet: Worksheet,
    width_mapping: Dict[str, int],
) -> None:
    """Queue column width updates."""
    for column, width in width_mapping.items():
        batcher.queue_column_width(column, width, worksheet)


def queue_overview_borders(
    batcher: SheetBatcher,
    worksheet: Worksheet,
    sheet_height: int,
) -> None:
    """Queue border formatting for the overview dashboard."""
    if sheet_height > 4:
        batcher.queue_border(
            f'B3:B{sheet_height - 2}',
            {'left': {'style': 'SOLID'}},
            worksheet,
        )
        batcher.queue_border(
            f'Y3:Y{sheet_height - 2}',
            {'right': {'style': 'SOLID'}},
            worksheet,
        )

    batcher.queue_border(
        'C2:X2',
        {'top': {'style': 'SOLID'}, 'bottom': {'style': 'SOLID'}},
        worksheet,
    )
    batcher.queue_border(
        f'C{sheet_height - 1}:X{sheet_height - 1}',
        {'bottom': {'style': 'SOLID'}},
        worksheet,
    )

    # Corners
    batcher.queue_border(
        'B2',
        {
            'left': {'style': 'SOLID'},
            'top': {'style': 'SOLID'},
            'bottom': {'style': 'SOLID'},
        },
        worksheet,
    )
    batcher.queue_border(
        'Y2',
        {
            'right': {'style': 'SOLID'},
            'top': {'style': 'SOLID'},
            'bottom': {'style': 'SOLID'},
        },
        worksheet,
    )
    batcher.queue_border(
        f'B{sheet_height - 1}',
        {'left': {'style': 'SOLID'}, 'bottom': {'style': 'SOLID'}},
        worksheet,
    )
    batcher.queue_border(
        f'Y{sheet_height - 1}',
        {'right': {'style': 'SOLID'}, 'bottom': {'style': 'SOLID'}},
        worksheet,
    )

    # Column dividers
    columns_to_format = ['C', 'D', 'F', 'K', 'L', 'P', 'T', 'W', 'X', 'Y']

    for col in columns_to_format:
        is_column_y = col == 'Y'
        sides_middle = {'left': {'style': 'SOLID'}}
        sides_top = {'left': {'style': 'SOLID'}, 'top': {'style': 'SOLID'}}
        sides_bottom = {'left': {'style': 'SOLID'}, 'bottom': {'style': 'SOLID'}}

        if is_column_y:
            sides_middle['right'] = {'style': 'SOLID'}
            sides_top['right'] = {'style': 'SOLID'}
            sides_bottom['right'] = {'style': 'SOLID'}

        if sheet_height > 5:
            batcher.queue_border(
                f'{col}4:{col}{sheet_height - 2}', sides_middle, worksheet
            )

        batcher.queue_border(f'{col}3', sides_top, worksheet)
        batcher.queue_border(f'{col}{sheet_height - 1}', sides_bottom, worksheet)


def refresh_overview_dashboard(
    sh: Spreadsheet,
    batcher: SheetBatcher,
    grain: str,
) -> None:
    """Refresh an overview dashboard (yearly or monthly)."""
    title = f'Overview - {grain.capitalize()}'
    logging.info(f'Updating {title}')

    column_label = grain.capitalize().replace('ly', '')

    df = get_df_from_table(f'dashboards.{grain}_level')
    df.columns = [column_label] + OVERVIEW_COLUMN_TITLES

    if grain == 'monthly':
        df[column_label] = to_datetime(df[column_label]).dt.strftime('%-m/%Y')

    sheet_height = len(df) + 3
    sheet_width = len(df.columns) + 2

    worksheet = create_worksheet(sh, title, sheet_height, sheet_width, batcher)

    overview_format = OVERVIEW_MONTHLY_FORMAT if grain == 'monthly' else OVERVIEW_YEARLY_FORMAT
    queue_df_to_sheet(batcher, df, worksheet, 'B2', overview_format)
    batcher.queue_format('B2:Y2', overview_format['B2:Y2'], worksheet)

    batcher.queue_columns_auto_resize(2, sheet_width, worksheet)
    queue_column_widths(batcher, worksheet, OVERVIEW_COLUMN_WIDTH_MAPPING)

    batcher.queue_notes(OVERVIEW_NOTES, worksheet)
    queue_last_updated_cell(batcher, worksheet)
    queue_overview_borders(batcher, worksheet, sheet_height)

    logging.info(f'{title} queued for update')


def refresh_yearly_categories_dashboards(
    sh: Spreadsheet,
    batcher: SheetBatcher,
) -> None:
    """Refresh all yearly categories dashboards."""
    years = sorted(
        get_df_from_table('dashboards.yearly_level')['budget_year'].values,
        reverse=True,
    )

    logging.info(
        f'Updating yearly categories dashboards for years: {", ".join(map(str, years))}'
    )

    for year in years:
        logging.info(f'Updating {year} - Categories')
        title = f'{year} - Categories'

        worksheet = create_worksheet(sh, title, 34, 16, batcher)

        df_needs = get_df_from_table(
            'dashboards.yearly_needs', f'budget_year = {year}'
        )[['subcategory_group', 'category_name', 'spend']]
        df_needs.columns = ['Subcategory Group', 'Needs', 'Spend']
        queue_df_to_sheet(batcher, df_needs, worksheet, 'E2')

        df_wants = get_df_from_table(
            'dashboards.yearly_wants', f'budget_year = {year}'
        )[['subcategory_group', 'category_name', 'spend']]
        df_wants.columns = ['Subcategory Group', 'Wants', 'Spend']
        queue_df_to_sheet(batcher, df_wants, worksheet, 'I2')

        df_savings = get_df_from_table(
            'dashboards.yearly_other', f'budget_year = {year}'
        )[['category_name', 'assigned', 'spend']]
        df_savings.columns = ['Other', 'Assigned', 'Spend']
        queue_df_to_sheet(batcher, df_savings, worksheet, 'M2')

        df_other = get_df_from_table(
            'dashboards.yearly_category_group', f'budget_year = {year}'
        )[['category_group_name_mapping', 'assigned', 'spend']]
        df_other.columns = ['Category Group', 'Assigned', 'Spend']
        queue_df_to_sheet(batcher, df_other, worksheet, 'M12')

        df_subcategory = get_df_from_table(
            'dashboards.yearly_subcategory_group', f'budget_year = {year}'
        )[['subcategory_group', 'assigned', 'spend']]
        df_subcategory.columns = ['Subcategory Group', 'Assigned', 'Spend']
        queue_df_to_sheet(batcher, df_subcategory, worksheet, 'M20')

        df_paycheck = get_df_from_table(
            'dashboards.yearly_paychecks', f'budget_year = {year}'
        )[['paycheck_column', 'paycheck_value']]
        df_paycheck.columns = ['Paycheck Value', 'Amount']
        queue_df_to_sheet(batcher, df_paycheck, worksheet, 'B2')

        queue_format_dict(batcher, worksheet, YEARLY_CATEGORIES_FORMAT)
        queue_column_widths(batcher, worksheet, YEARLY_CATEGORIES_COLUMN_WIDTH_MAPPING)
        batcher.queue_notes(YEARLY_CATEGORIES_NOTES, worksheet)
        queue_last_updated_cell(batcher, worksheet)

        logging.info(f'{year} - Categories queued for update')


def refresh_sheets() -> None:
    """Main function to refresh all Google Sheets dashboards."""
    credentials_dict = json.loads(os.getenv('GSPREAD_CREDENTIALS').replace('\n', '\\n'))
    gc = service_account_from_dict(credentials_dict)
    sh = gc.open('Spending Dashboard')

    logging.info('Starting sheet refresh with batch operations')

    with SheetBatcher(sh) as batcher:
        logging.info('Refreshing overview dashboards')
        try:
            refresh_overview_dashboard(sh, batcher, 'yearly')
        except Exception as e:
            logging.error(f'Failed to queue yearly overview dashboard: {e}')

        try:
            refresh_overview_dashboard(sh, batcher, 'monthly')
        except Exception as e:
            logging.error(f'Failed to queue monthly overview dashboard: {e}')

        logging.info('Refreshing yearly categories dashboards')
        try:
            refresh_yearly_categories_dashboards(sh, batcher)
        except Exception as e:
            logging.error(f'Failed to queue yearly categories dashboards: {e}')

    logging.info('Sheet refresh complete')
