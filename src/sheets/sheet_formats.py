OVERVIEW_COLUMN_WIDTH_MAPPING = {
    'A': 21,
    'B': 60,
    'C': 85,
    'D': 82,
    'E': 82,
    'F': 100,
    'G': 85,
    'H': 110,
    'I': 110,
    'J': 100,
    'K': 100,
    'L': 80,
    'M': 100,
    'N': 120,
    'O': 120,
    'P': 85,
    'Q': 85,
    'R': 85,
    'S': 95,
    'T': 75,
    'U': 100,
    'V': 100,
    'W': 95,
    'X': 85,
    'Y': 80,
    'Z': 21,
}
OVERVIEW_COLUMN_TITLES = [
    'Pre-Tax Earnings',
    'Salary',
    'Bonus',
    'Pre-Tax Deductions',
    'Taxes',
    'Retirement Fund Contribution',
    'HSA Contribution',
    'Post-Tax Deductions',
    'Total Deductions',
    'Net Pay',
    'Reimbursed Income',
    'Miscellaneous Income',
    'Total Income (Net to Account)',
    'Needs Spend',
    'Wants Spend',
    'Savings Spend',
    'Emergency Fund Spend',
    'Savings Saved',
    'Emergency Fund Saved',
    'Investments Saved',
    'Emergency Fund in HSA',
    'Total Spend',
    'Net Income',
]
CURRENCY_FORMAT = {
    'horizontalAlignment': 'RIGHT',
    'numberFormat': {
        'type': 'CURRENCY',
        'pattern': '[Color 10]$#,##0.00;[Color 9]($#,##0.00);$0.00',
    },
}
HEADER_FORMAT = {
    'horizontalAlignment': 'CENTER',
    'wrapStrategy': 'WRAP',
    'textFormat': {'fontSize': 12, 'bold': True},
}
HEADER_FORMAT_NO_WRAP = {
    'horizontalAlignment': 'CENTER',
    'textFormat': {'fontSize': 12, 'bold': True},
}
LEFT_ALIGN_PLAIN_TEXT = {
    'horizontalAlignment': 'LEFT',
    'hyperlinkDisplayType': 'PLAIN_TEXT',
}

OVERVIEW_MONTHLY_FORMAT = {
    'B2:Y2': HEADER_FORMAT,
    'B3:B': {
        'horizontalAlignment': 'RIGHT',
        'numberFormat': {'type': 'DATE', 'pattern': 'MM/yyyy'},
    },
    'C3:Y': CURRENCY_FORMAT,
}
OVERVIEW_YEARLY_FORMAT = {
    'B2:Y2': HEADER_FORMAT,
    'B3:B': {
        'horizontalAlignment': 'RIGHT',
    },
    'C3:Y': CURRENCY_FORMAT,
}
OVERVIEW_NOTES = {
    'C2': 'Gross Earnings',
    'E2': 'Includes Bonus, PTO Payout, and Severance',
    'F2': 'FSA and Medical',
    'G2': 'Medicare, Federal, State, City, NYPFL, Disability, and Social Security',
    'H2': 'Pre Tax or Roth',
    'J2': 'Critical Illness, AD & D, Long Term Disability',
    'K2': 'Pre-Tax Deductions + Taxes + HSA Contribution + Post-Tax Deductions',
    'L2': 'Gross Earnings - Total Deductions',
    'M2': 'Income for Items Reimbursed by Employer. Not counted in Total Income because the spend is categorized as a net zero expense category.',
    'N2': 'Any income not from an employer. Credit Card Cash Back, Selling items, gifts, etc.',
    'O2': 'Net Pay + Miscellaneous Income',
    'V2': 'Investment categories equal 0 at the end of the month and all money budgeted is invested.',
    'W2': 'Amount spent on items that can be reimbursed by the HSA, but have not been reimbursed yet. This money can be used for emergency expenses as a last resort.',
    'X2': 'Needs Spend + Wants Spend + Savings Spend + Emergency Fund Spend',
    'Y2': 'Total Income (Net to Account) - Total Spend',
}

PERCENT_FORMAT = {
    'horizontalAlignment': 'RIGHT',
    'numberFormat': {'type': 'PERCENT', 'pattern': '0.0%'},
}
MONTHS_FORMAT = {
    'horizontalAlignment': 'RIGHT',
    'numberFormat': {'type': 'NUMBER', 'pattern': '0.00'},
}

OFF_PLAN_ORANGE_FILL = {'red': 0.99, 'green': 0.68, 'blue': 0.34}
DEFICIT_RED_FILL = {'red': 0.96, 'green': 0.80, 'blue': 0.80}


def off_plan_orange_rule(row_range: str, flag_col: str, first_data_row: int) -> dict:
    """Orange-fill a bucket row when its on_plan_flag cell is FALSE."""
    return {
        'range': row_range,
        'type': 'CUSTOM_FORMULA',
        'values': [f'=${flag_col}{first_data_row}=FALSE'],
        'format': {'backgroundColor': OFF_PLAN_ORANGE_FILL},
    }


def deficit_red_rule(value_range: str, value_col: str, first_data_row: int) -> dict:
    """Red-fill a period's net-income cell when it is a deficit (< 0)."""
    return {
        'range': value_range,
        'type': 'CUSTOM_FORMULA',
        'values': [f'=${value_col}{first_data_row}<0'],
        'format': {'backgroundColor': DEFICIT_RED_FILL},
    }


def zone_band_rules(zone_range: str, zone_col: str, first_data_row: int) -> list[dict]:
    """Color runway zone cells by band: Unhealthy -> red, Watch -> orange."""
    return [
        {
            'range': zone_range,
            'type': 'CUSTOM_FORMULA',
            'values': [f'=${zone_col}{first_data_row}="Unhealthy"'],
            'format': {'backgroundColor': DEFICIT_RED_FILL},
        },
        {
            'range': zone_range,
            'type': 'CUSTOM_FORMULA',
            'values': [f'=${zone_col}{first_data_row}="Watch"'],
            'format': {'backgroundColor': OFF_PLAN_ORANGE_FILL},
        },
    ]


# Tab 1 — Income Derivation (dashboards.income_derivation); columns B..O
INCOME_DERIVATION_COLUMN_TITLES = [
    'Salary',
    'Estimated Tax',
    'HSA',
    'Extra Income',
    'Allocatable Income',
    'Needs Target (50%)',
    'Wants Target (30%)',
    'Investments Target (15%)',
    'Savings Target (5%)',
    'Match Milestone Hit',
    '401k Employee Remaining',
    'Net Income',
    'Data Type',
]
INCOME_DERIVATION_FORMAT = {
    'B2:O2': HEADER_FORMAT,
    'B3:B': {'horizontalAlignment': 'RIGHT'},
    'C3:K': CURRENCY_FORMAT,
    'M3:N': CURRENCY_FORMAT,
}
INCOME_DERIVATION_NOTES = {
    'F2': 'Net-to-account one-off income (bonus, refunds, interest, cashback). '
    'Actual YTD, never extrapolated; does not feed Allocatable Income.',
    'G2': 'Salary - Estimated Tax - HSA. Bucket targets split this 50/30/15/5.',
    'L2': '401k employee elective-limit hit; closest available proxy for '
    'capturing the full employer match.',
    'N2': 'Total Income (Net to Account) - Total Spend. Red when in deficit.',
}
INCOME_DERIVATION_COLUMN_WIDTH_MAPPING = {
    'A': 21,
    'B': 60,
    'C': 100,
    'D': 100,
    'E': 90,
    'F': 100,
    'G': 110,
    'H': 100,
    'I': 100,
    'J': 110,
    'K': 100,
    'L': 95,
    'M': 110,
    'N': 100,
    'O': 95,
    'P': 21,
}

# Tab 2 — Bucket Adherence (dashboards.bucket_adherence); columns B..H
BUCKET_ADHERENCE_COLUMN_TITLES = [
    'Bucket',
    'Target',
    'Projected',
    'Overage %',
    'On Plan',
    'Data Type',
]
BUCKET_ADHERENCE_COLUMN_FORMATS = {
    'Target': CURRENCY_FORMAT,
    'Projected': CURRENCY_FORMAT,
    'Overage %': PERCENT_FORMAT,
}
BUCKET_ADHERENCE_NOTES = {
    'C2': 'Needs/Wants are less-is-good; Investments/Savings are more-is-good. '
    'Rows turn orange when off plan.',
    'D2': 'Investments target includes the §8 extra-income surplus.',
}
BUCKET_ADHERENCE_COLUMN_WIDTH_MAPPING = {
    'A': 21,
    'B': 60,
    'C': 100,
    'D': 100,
    'E': 100,
    'F': 90,
    'G': 75,
    'H': 95,
    'I': 110,
}

# Tab 2 — §8 extra-income strip (dashboards.extra_income_allocation); columns B..I
EXTRA_INCOME_ALLOCATION_COLUMN_TITLES = [
    'Extra Income',
    'Used: Emergency Fund',
    'Used: Needs Overage',
    'Used: Wants Overage',
    'Used: Savings Coverage',
    'Surplus to Investments',
    'Data Type',
]
EXTRA_INCOME_ALLOCATION_COLUMN_FORMATS = {
    'Extra Income': CURRENCY_FORMAT,
    'Used: Emergency Fund': CURRENCY_FORMAT,
    'Used: Needs Overage': CURRENCY_FORMAT,
    'Used: Wants Overage': CURRENCY_FORMAT,
    'Used: Savings Coverage': CURRENCY_FORMAT,
    'Surplus to Investments': CURRENCY_FORMAT,
}

# Tab 3 — Investments (dashboards.investments); columns B..Q
INVESTMENTS_COLUMN_TITLES = [
    '401k Contributions',
    '401k Employee Contributions',
    'Taxable Contributions',
    'Total Invested',
    '401k Contribution Limit',
    '401k Limit Hit',
    '401k Split Target',
    'Taxable Split Target',
    '401k Split %',
    'Taxable Split %',
    'Investments Target (15%)',
    'Remaining to Target',
    'Extra Income Surplus',
    'Emergency Top-Ups',
    'Data Type',
]
INVESTMENTS_FORMAT = {
    'B2:Q2': HEADER_FORMAT,
    'B3:B': {'horizontalAlignment': 'RIGHT'},
    'C3:G': CURRENCY_FORMAT,
    'I3:J': CURRENCY_FORMAT,
    'K3:L': PERCENT_FORMAT,
    'M3:P': CURRENCY_FORMAT,
}
INVESTMENTS_NOTES = {
    'F2': '401k Contributions + Taxable Contributions.',
    'H2': '401k employee elective-deferral limit reached.',
    'I2': '50/50 split of the target with employee-limit spillover to taxable.',
    'M2': '15% of Allocatable Income plus the §8 extra-income surplus is the '
    'with-surplus target tracked on the Bucket Adherence tab.',
    'P2': 'Extra income recognized as Emergency Fund contributions (§8 priority 1).',
}
INVESTMENTS_COLUMN_WIDTH_MAPPING = {
    'A': 21,
    'B': 60,
    'C': 110,
    'D': 120,
    'E': 110,
    'F': 100,
    'G': 110,
    'H': 80,
    'I': 100,
    'J': 100,
    'K': 80,
    'L': 80,
    'M': 110,
    'N': 100,
    'O': 100,
    'P': 100,
    'Q': 95,
    'R': 21,
}

# Tab 4 — Category Drilldown (dashboards.category_drilldown); columns B..H
CATEGORY_DRILLDOWN_COLUMN_TITLES = [
    'Bucket',
    'Category',
    'Amount Spent',
    'Projected',
    '% of Bucket',
    'Data Type',
]
CATEGORY_DRILLDOWN_FORMAT = {
    'B2:H2': HEADER_FORMAT,
    'B3:B': {'horizontalAlignment': 'RIGHT'},
    'E3:F': CURRENCY_FORMAT,
    'G3:G': PERCENT_FORMAT,
}
CATEGORY_DRILLDOWN_NOTES = {
    'F2': 'Current-year rows extrapolate YTD spend across elapsed months.',
    'G2': "Category share of its bucket's projected spend.",
}
CATEGORY_DRILLDOWN_COLUMN_WIDTH_MAPPING = {
    'A': 21,
    'B': 60,
    'C': 80,
    'D': 180,
    'E': 100,
    'F': 100,
    'G': 90,
    'H': 95,
    'I': 21,
}

# Tab 5 — Runway (dashboards.runway); columns B..K + sparkline at M
RUNWAY_COLUMN_TITLES = [
    'Liquid Cash',
    'Emergency Fund Balance',
    'HSA Reimbursable Reserve',
    'Runway (3mo)',
    'Runway (12mo)',
    'Runway (Projected)',
    'Worst Runway',
    'Zone',
    'Data Type',
]
RUNWAY_FORMAT = {
    'B2:K2': HEADER_FORMAT,
    'B3:B': {'horizontalAlignment': 'RIGHT'},
    'C3:E': CURRENCY_FORMAT,
    'F3:I': MONTHS_FORMAT,
}
RUNWAY_NOTES = {
    'C2': 'As-of bank balance minus Emergency Fund balance and Savings assigned.',
    'D2': 'Primary reserve: cash Emergency Fund group balance.',
    'E2': 'Secondary reserve: HSA-reimbursable spend not yet reimbursed.',
    'I2': 'Worst of the three runway views; N/A when burn is zero.',
    'J2': 'Healthy >= 2.00 months, Watch 1.50-2.00, Unhealthy < 1.50.',
}
RUNWAY_COLUMN_WIDTH_MAPPING = {
    'A': 21,
    'B': 60,
    'C': 100,
    'D': 120,
    'E': 130,
    'F': 90,
    'G': 90,
    'H': 100,
    'I': 90,
    'J': 80,
    'K': 95,
    'L': 21,
    'M': 220,
}
