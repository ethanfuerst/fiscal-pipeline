select
    budget_month
    , earnings_actual
    , pre_tax_deductions
    , taxes
    , retirement_fund
    , hsa
    , post_tax_deductions
    , total_deductions
    , net_pay
    , income_for_reimbursements
    , misc_income
    , total_income
    , needs_spend
    , wants_spend
    , savings_spend
    , emergency_fund_spend
    , savings_saved
    , emergency_fund_saved
    , investments_saved
    , emergency_fund_in_hsa
    , spent
    , difference
from fiscal_pipeline.dashboards.monthly_level
