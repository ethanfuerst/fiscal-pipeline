from datetime import datetime, timezone
from typing import Any, Dict, List

from eftoolkit.gsheets.runner import (
    CellLocation,
    HookContext,
    WorksheetAsset,
    WorksheetFormatting,
)
from pandas import DataFrame, to_datetime

from src.sheets.sheet_formats import CURRENCY_FORMAT, HEADER_FORMAT

WATERFALL_COLUMN_TITLES = {
    'budget_month': 'Month',
    'budget_year': 'Year',
    'net_income_to_account': 'Net Income',
    'plan_income': 'Plan Income',
    'target_needs_spend': 'Needs Target',
    'actual_needs_spend': 'Needs Spend',
    'needs_surplus_spend': 'Needs Surplus',
    'target_wants_spend': 'Wants Target',
    'actual_wants_spend': 'Wants Spend',
    'wants_surplus_spend': 'Wants Surplus',
    'target_savings_saved': 'Savings Target',
    'actual_savings_saved': 'Savings Saved',
    'savings_surplus_saved': 'Savings Surplus',
    'savings_balance_change_saved': 'Savings Net',
    'savings_rolling_balance_saved': 'Rolling Savings Balance',
    'target_investments_saved': 'Investments Target',
    'actual_investments_saved': 'Investments Saved',
    'investments_surplus_saved': 'Investments Surplus',
    'overflow_dollars_saved': 'Overflow Dollars',
    'buffer_target_saved': 'Buffer Target',
    'buffer_balance_saved': 'Buffer Balance',
    'buffer_gap_saved': 'Buffer Gap',
    'buffer_surplus_saved': 'Buffer Surplus',
    'overflow_to_buffer_saved': 'Overflow to Buffer',
    'used_from_buffer_saved': 'Used From Buffer',
    'uncovered_shortfall_spend': 'Uncovered Shortfall',
    'true_excess_saved': 'True Excess',
    'emergency_fund_target_saved': 'Emergency Fund Target',
    'emergency_fund_gap_saved': 'Emergency Fund Gap',
    'emergency_fund_surplus_saved': 'Emergency Fund Surplus',
    'reserve_surplus_saved': 'Reserve Surplus',
    'other_savings_balance_saved': 'Other Savings Balance',
    'actual_rollover': 'Actual Rollover',
    'runway_budget_month': 'Runway Month',
    'runway_cash_saved': 'Runway Cash',
    'emergency_fund_balance': 'Emergency Fund Balance',
    'monthly_cash_spend': 'Monthly Cash Spend',
    'avg_complete_monthly_cash_spend_3mo': 'Avg Monthly Cash Spend - Prior 3 Months',
    'target_total_runway_cash_saved': 'Target Total Runway Cash',
    'required_rollover_cash_saved': 'Required Rollover Cash',
    'rollover_runway_months': 'Rollover Runway Months',
    'total_runway_months': 'Total Runway Months',
    'rollover_cash_shortfall_saved': 'Rollover Cash Shortfall',
    'excess_rollover_cash_saved': 'Excess Rollover Cash',
    'has_balanced_runway': 'Has Balanced Runway',
    'needs_gap_covered_saved': 'Needs Gap Covered',
    'needs_gap_uncovered_spend': 'Needs Gap Uncovered',
    'excess_cash_after_needs_saved': 'Excess Cash After Needs',
    'wants_gap_covered_saved': 'Wants Gap Covered',
    'wants_gap_uncovered_spend': 'Wants Gap Uncovered',
    'excess_cash_after_wants_saved': 'Excess Cash After Wants',
    'savings_gap_covered_saved': 'Savings Gap Covered',
    'savings_gap_uncovered_saved': 'Savings Gap Uncovered',
    'excess_cash_after_savings_saved': 'Excess Cash After Savings',
    'investments_gap_covered_saved': 'Investments Gap Covered',
    'investments_gap_uncovered_saved': 'Investments Gap Uncovered',
    'remaining_excess_cash_saved': 'Remaining Excess Cash',
    'total_cash_available_to_invest_saved': 'Cash Available to Invest',
}

WATERFALL_VISIBLE_SOURCE_COLUMNS = [
    'budget_month',
    'budget_year',
    'net_income_to_account',
    'target_needs_spend',
    'actual_needs_spend',
    'needs_surplus_spend',
    'target_wants_spend',
    'actual_wants_spend',
    'wants_surplus_spend',
    'target_savings_saved',
    'actual_savings_saved',
    'savings_surplus_saved',
    'savings_balance_change_saved',
    'savings_rolling_balance_saved',
    'target_investments_saved',
    'actual_investments_saved',
    'investments_surplus_saved',
    'overflow_dollars_saved',
    'buffer_balance_saved',
    'buffer_target_saved',
    'buffer_surplus_saved',
    'emergency_fund_target_saved',
    'emergency_fund_balance',
    'emergency_fund_surplus_saved',
    'reserve_surplus_saved',
    'other_savings_balance_saved',
    'used_from_buffer_saved',
]

WATERFALL_HIDDEN_SOURCE_COLUMNS = [
    'actual_rollover',
    'runway_budget_month',
    'runway_cash_saved',
    'target_total_runway_cash_saved',
    'required_rollover_cash_saved',
    'rollover_runway_months',
    'total_runway_months',
    'rollover_cash_shortfall_saved',
    'excess_rollover_cash_saved',
    'has_balanced_runway',
    'buffer_gap_saved',
    'true_excess_saved',
    'overflow_to_buffer_saved',
    'emergency_fund_gap_saved',
    'uncovered_shortfall_spend',
    'monthly_cash_spend',
    'avg_complete_monthly_cash_spend_3mo',
    'needs_gap_covered_saved',
    'needs_gap_uncovered_spend',
    'excess_cash_after_needs_saved',
    'wants_gap_covered_saved',
    'wants_gap_uncovered_spend',
    'excess_cash_after_wants_saved',
    'savings_gap_covered_saved',
    'savings_gap_uncovered_saved',
    'excess_cash_after_savings_saved',
    'investments_gap_covered_saved',
    'investments_gap_uncovered_saved',
    'remaining_excess_cash_saved',
    'total_cash_available_to_invest_saved',
]

WATERFALL_NOTES_BY_TITLE = {
    'Month': 'Budget month for this row.',
    'Year': 'Budget year for this row.',
    'Net Income': 'Income deposited to budget accounts for the period.',
    'Needs Target': '50% of Net Income. This is the planned maximum for Needs spending.',
    'Needs Spend': 'Actual Needs spending for the period, shown as a negative number.',
    'Needs Surplus': 'Needs Target plus Needs Spend. Positive means under target; negative means over target.',
    'Wants Target': '30% of Net Income. This is the planned maximum for Wants spending.',
    'Wants Spend': 'Actual Wants spending for the period, shown as a negative number.',
    'Wants Surplus': 'Wants Target plus Wants Spend. Positive means under target; negative means over target.',
    'Savings Target': '5% of Net Income. This is the planned minimum for Savings and Emergency Fund assignments.',
    'Savings Saved': 'Money assigned to Savings and Emergency Fund categories, shown as a negative number to compare against the target.',
    'Savings Surplus': 'Savings Target plus Savings Saved. Positive means under the 5% savings target; negative means more than target was assigned.',
    'Savings Net': 'Savings and Emergency Fund assignments minus spending from those categories. This shows whether their balances rose or fell.',
    'Rolling Savings Balance': 'Month-end balance of Savings plus Emergency Fund categories in YNAB.',
    'Investments Target': '15% of Net Income. This is the planned minimum for taxable investment assignments.',
    'Investments Saved': 'Money assigned to taxable investment categories, shown as a negative number to compare against the target.',
    'Investments Surplus': 'Investments Target plus Investments Saved. Positive means under the 15% target; negative means more than target was assigned.',
    'Overflow Dollars': 'Net Income minus current Needs/Wants spend and saved dollars.',
    'Buffer Target': 'Average Needs/Wants spend from the current year once six complete months exist; otherwise the trailing six complete months.',
    'Buffer Balance': 'Needs/Wants available at month end plus income received but not yet assigned to any month through month end. Matches current Needs/Wants money in YNAB including pre-funded future months.',
    'Buffer Gap': 'Additional Buffer Balance needed to cover the one-month Buffer Target.',
    'Buffer Surplus': 'Buffer Balance minus Buffer Target. Positive means the buffer is over target; negative means it is under target.',
    'Overflow to Buffer': 'Current-period positive Overflow Dollars needed to fill the Buffer Gap.',
    'Used From Buffer': 'Current-period negative Overflow Dollars covered by the starting Buffer Balance.',
    'Uncovered Shortfall': 'Current-period negative Overflow Dollars not covered by the starting Buffer Balance.',
    'True Excess': 'Buffer Balance above the one-month Buffer Target.',
    'Emergency Fund Target': 'Buffer Target multiplied by 3.',
    'Emergency Fund Gap': 'Additional cash Emergency Fund balance needed to hit the three-month target.',
    'Emergency Fund Surplus': 'Emergency Fund Balance minus Emergency Fund Target. HSA reimbursement value is not included.',
    'Reserve Surplus': 'Emergency Fund Surplus after applying any Buffer shortfall or excess. This is the combined cash reserve amount to drive toward zero.',
    'Other Savings Balance': 'Savings category balance outside the Emergency Fund cash balance.',
    'Actual Rollover': 'Net income left after actual budget-account spend and budgeted saved amounts.',
    'Runway Month': 'Latest month represented in the yearly runway metrics.',
    'Runway Cash': 'Net income not assigned in the budget month; assumed to be budgeted into future months.',
    'Emergency Fund Balance': 'Emergency Fund balance included in total runway.',
    'Monthly Cash Spend': 'Actual Needs and Wants cash spend used for runway calculations.',
    'Avg Monthly Cash Spend - Prior 3 Months': 'Average Needs and Wants cash spend from the prior three complete months.',
    'Target Total Runway Cash': '3.5 months of prior-average spend across rollover cash plus Emergency Fund.',
    'Required Rollover Cash': 'Rollover cash required to satisfy both one month of rollover cash and 3.5 months of total runway.',
    'Rollover Runway Months': 'Months of prior-average spend covered by runway cash.',
    'Total Runway Months': 'Months of prior-average spend covered by runway cash plus Emergency Fund.',
    'Rollover Cash Shortfall': 'Additional runway cash needed before surplus can fill category gaps.',
    'Excess Rollover Cash': 'Runway cash above the protected runway target.',
    'Has Balanced Runway': 'True when runway cash satisfies both runway constraints.',
    'Needs Gap Covered': 'Excess cash used to cover Needs overspend before lower-priority gaps.',
    'Needs Gap Uncovered': 'Needs overspend still uncovered after excess cash.',
    'Excess Cash After Needs': 'Excess cash remaining after Needs gap coverage.',
    'Wants Gap Covered': 'Excess cash used to cover Wants overspend after Needs.',
    'Wants Gap Uncovered': 'Wants overspend still uncovered after excess cash.',
    'Excess Cash After Wants': 'Excess cash remaining after Wants gap coverage.',
    'Savings Gap Covered': 'Excess cash used to cover the Savings target shortfall after spending gaps.',
    'Savings Gap Uncovered': 'Savings target shortfall still uncovered after excess cash.',
    'Excess Cash After Savings': 'Excess cash remaining after Savings gap coverage.',
    'Investments Gap Covered': 'Excess cash used to cover the Investments target shortfall after higher-priority gaps.',
    'Investments Gap Uncovered': 'Investments target shortfall still uncovered after excess cash.',
    'Remaining Excess Cash': 'Excess cash left after all target gaps are filled.',
    'Cash Available to Invest': 'Cash available for retroactive investment budgeting after higher-priority gaps and runway targets.',
}

NUMBER_FORMAT = {
    'horizontalAlignment': 'RIGHT',
    'numberFormat': {'type': 'NUMBER', 'pattern': '#,##0.00'},
}
CENTER_FORMAT = {'horizontalAlignment': 'CENTER'}
YEAR_FORMAT = {'horizontalAlignment': 'RIGHT'}
MONTH_FORMAT = {
    'horizontalAlignment': 'RIGHT',
    'numberFormat': {'type': 'DATE', 'pattern': 'MM/yyyy'},
}


class AdheranceWaterfallWorksheetBase:
    grain: str

    @property
    def name(self) -> str:
        return f'Adherence Waterfall - {self.grain.capitalize()}'

    @property
    def table_name(self) -> str:
        return f'dashboards.{self.grain}_adherance_waterfall'

    def load_df(self, db) -> DataFrame:
        df = db.get_table(self.table_name)
        df = df.drop(
            columns=['plan_income', *WATERFALL_HIDDEN_SOURCE_COLUMNS], errors='ignore'
        )
        visible_columns = [
            column
            for column in WATERFALL_VISIBLE_SOURCE_COLUMNS
            if column in df.columns
        ]
        df = df[visible_columns]
        df = df.rename(columns=WATERFALL_COLUMN_TITLES)

        if self.grain == 'monthly':
            df['Month'] = to_datetime(df['Month']).dt.strftime('%-m/%Y')
        if 'Runway Month' in df.columns:
            df['Runway Month'] = to_datetime(df['Runway Month']).dt.strftime('%-m/%Y')

        return df

    def format_and_stamp_hook(self, ctx: HookContext) -> None:
        last_col = column_letter(len(ctx.asset.df.columns) + 1)
        ctx.worksheet.format_range(f'B2:{last_col}2', HEADER_FORMAT)
        ctx.worksheet.format_range(
            'B3:B', MONTH_FORMAT if self.grain == 'monthly' else YEAR_FORMAT
        )

        for col_idx, title in enumerate(ctx.asset.df.columns, start=2):
            col = column_letter(col_idx)
            if title in {'Month', 'Year'}:
                continue
            if title == 'Runway Month':
                ctx.worksheet.format_range(f'{col}3:{col}', MONTH_FORMAT)
            elif title == 'Has Balanced Runway':
                ctx.worksheet.format_range(f'{col}3:{col}', CENTER_FORMAT)
            elif title in {'Rollover Runway Months', 'Total Runway Months'}:
                ctx.worksheet.format_range(f'{col}3:{col}', NUMBER_FORMAT)
            else:
                ctx.worksheet.format_range(f'{col}3:{col}', CURRENCY_FORMAT)

        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
        ctx.worksheet.write_values('B1', [[f'Last Updated: {timestamp}']])

    def trim_to_data_hook(self, ctx: HookContext) -> None:
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

    def get_formatting(self, context: dict) -> WorksheetFormatting | None:
        assets = context[self.name]['assets']
        if not assets:
            return None

        num_rows = assets[0].num_rows
        sheet_height = num_rows + 3
        sheet_width = len(assets[0].df.columns) + 2
        last_col = column_letter(sheet_width - 1)

        return WorksheetFormatting(
            notes=waterfall_notes(assets[0].df.columns),
            column_widths=waterfall_column_widths(assets[0].df.columns),
            borders=waterfall_borders(sheet_height, last_col, assets[0].df.columns),
            auto_resize_columns=(2, sheet_width),
        )


def column_letter(index: int) -> str:
    letters = ''
    while index:
        index, remainder = divmod(index - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters


def waterfall_notes(columns) -> Dict[str, str]:
    return {
        f'{column_letter(col_idx)}2': WATERFALL_NOTES_BY_TITLE[title]
        for col_idx, title in enumerate(columns, start=2)
        if title in WATERFALL_NOTES_BY_TITLE
    }


def waterfall_column_widths(columns) -> Dict[str, int]:
    widths = {'A': 21, column_letter(len(columns) + 2): 21}

    for col_idx, title in enumerate(columns, start=2):
        col = column_letter(col_idx)
        if title in {'Month', 'Year'}:
            widths[col] = 70
        elif title == 'Has Balanced Runway':
            widths[col] = 90
        elif 'Runway Months' in title:
            widths[col] = 95
        elif len(title) > 26:
            widths[col] = 135
        else:
            widths[col] = 110

    return widths


def waterfall_borders(
    sheet_height: int, last_col: str, columns
) -> Dict[str, Dict[str, Any]]:
    borders: Dict[str, Dict[str, Any]] = {
        f'B2:{last_col}2': {
            'top': {'style': 'SOLID'},
            'bottom': {'style': 'SOLID'},
        },
        f'B{sheet_height - 1}:{last_col}{sheet_height - 1}': {
            'bottom': {'style': 'SOLID'}
        },
    }

    if sheet_height > 4:
        borders[f'B3:B{sheet_height - 2}'] = {'left': {'style': 'SOLID'}}
        borders[f'{last_col}3:{last_col}{sheet_height - 2}'] = {
            'right': {'style': 'SOLID'}
        }

    borders['B2'] = {
        'left': {'style': 'SOLID'},
        'top': {'style': 'SOLID'},
        'bottom': {'style': 'SOLID'},
    }
    borders[f'{last_col}2'] = {
        'right': {'style': 'SOLID'},
        'top': {'style': 'SOLID'},
        'bottom': {'style': 'SOLID'},
    }
    borders[f'B{sheet_height - 1}'] = {
        'left': {'style': 'SOLID'},
        'bottom': {'style': 'SOLID'},
    }
    borders[f'{last_col}{sheet_height - 1}'] = {
        'right': {'style': 'SOLID'},
        'bottom': {'style': 'SOLID'},
    }

    divider_titles = {
        'Net Income',
        'Needs Target',
        'Wants Target',
        'Savings Target',
        'Investments Target',
        'Overflow Dollars',
        'Buffer Balance',
        'Emergency Fund Target',
        'Used From Buffer',
    }
    for col_idx, title in enumerate(columns, start=2):
        if title in divider_titles:
            borders.update(
                waterfall_divider_borders(column_letter(col_idx), sheet_height)
            )

    return borders


def waterfall_divider_borders(col: str, sheet_height: int) -> Dict[str, Dict[str, Any]]:
    borders: Dict[str, Dict[str, Any]] = {
        f'{col}3': {
            'left': {'style': 'SOLID'},
            'top': {'style': 'SOLID'},
        },
        f'{col}{sheet_height - 1}': {
            'left': {'style': 'SOLID'},
            'bottom': {'style': 'SOLID'},
        },
    }

    if sheet_height > 5:
        borders[f'{col}4:{col}{sheet_height - 2}'] = {'left': {'style': 'SOLID'}}

    return borders


class AdheranceWaterfallYearlyWorksheet(AdheranceWaterfallWorksheetBase):
    grain = 'yearly'


class AdheranceWaterfallMonthlyWorksheet(AdheranceWaterfallWorksheetBase):
    grain = 'monthly'
