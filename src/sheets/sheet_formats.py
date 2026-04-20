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
