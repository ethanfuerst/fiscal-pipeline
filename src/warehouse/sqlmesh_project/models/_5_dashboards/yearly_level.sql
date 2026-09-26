MODEL (
  name dashboards.yearly_level,
  kind FULL,
  grain budget_year,
  description 'Yearly dashboard overview aggregated from dashboards.monthly_level. Preserves the legacy signed dashboard contract: deduction/spend columns are negative, income/saved columns are positive.'
);

select
    extract('year' from monthly_level_dashboard.budget_month) as budget_year  -- Calendar budget year
    , sum(monthly_level_dashboard.earnings_actual) as earnings_actual  -- Gross earnings, positive USD
    , sum(monthly_level_dashboard.salary) as salary  -- Salary earnings, positive USD
    , sum(monthly_level_dashboard.bonus) as bonus  -- Bonus/PTO/severance earnings, positive USD
    , sum(monthly_level_dashboard.pre_tax_deductions) as pre_tax_deductions  -- Pre-tax FSA/medical deductions, signed negative for dashboard display
    , sum(monthly_level_dashboard.taxes) as taxes  -- Paycheck taxes, signed negative for dashboard display
    , sum(monthly_level_dashboard.retirement_fund) as retirement_fund  -- Retirement contributions, signed negative for dashboard display
    , sum(monthly_level_dashboard.hsa) as hsa  -- HSA contributions, signed negative for dashboard display
    , sum(monthly_level_dashboard.post_tax_deductions)
        as post_tax_deductions  -- Post-tax insurance deductions, signed negative for dashboard display
    , sum(monthly_level_dashboard.total_deductions) as total_deductions  -- Total paycheck withholdings, signed negative for dashboard display
    , sum(monthly_level_dashboard.net_pay) as net_pay  -- Net pay excluding reimbursements, positive USD
    , sum(monthly_level_dashboard.income_for_reimbursements)
        as income_for_reimbursements  -- Non-taxable reimbursement income, positive USD
    , sum(monthly_level_dashboard.misc_income) as misc_income  -- Income-category inflow not explained by net pay, positive/negative USD
    , sum(monthly_level_dashboard.total_income) as total_income  -- Net income-category ledger activity, positive for net inflow
    , sum(monthly_level_dashboard.needs_spend) as needs_spend  -- Needs net spend, signed negative for dashboard display
    , sum(monthly_level_dashboard.wants_spend) as wants_spend  -- Wants net spend, signed negative for dashboard display
    , sum(monthly_level_dashboard.savings_spend) as savings_spend  -- Savings net spend, signed negative for dashboard display
    , sum(monthly_level_dashboard.emergency_fund_spend)
        as emergency_fund_spend  -- Emergency Fund net spend, signed negative for dashboard display
    , sum(monthly_level_dashboard.savings_saved) as savings_saved  -- Amount assigned to Savings categories, positive USD
    , sum(monthly_level_dashboard.emergency_fund_saved)
        as emergency_fund_saved  -- Amount assigned to Emergency Fund categories, positive USD
    , sum(monthly_level_dashboard.investments_saved) as investments_saved  -- Amount assigned to Investments categories, positive USD
    , sum(monthly_level_dashboard.spent) as spent  -- Total Needs/Wants/Savings/Emergency Fund spend, signed negative for dashboard display
    , sum(monthly_level_dashboard.difference) as difference  -- Total income plus signed spend, positive/negative USD
from dashboards.monthly_level as monthly_level_dashboard
group by 1
order by budget_year desc
