MODEL (
  name dashboards.yearly_level,
  kind FULL,
  grain budget_year
);

select
    extract('year' from monthly_level_dashboard.budget_month) as budget_year
    , sum(monthly_level_dashboard.earnings_actual) as earnings_actual
    , sum(monthly_level_dashboard.salary) as salary
    , sum(monthly_level_dashboard.bonus) as bonus
    , sum(monthly_level_dashboard.pre_tax_deductions) as pre_tax_deductions
    , sum(monthly_level_dashboard.taxes) as taxes
    , sum(monthly_level_dashboard.retirement_fund) as retirement_fund
    , sum(monthly_level_dashboard.hsa) as hsa
    , sum(monthly_level_dashboard.post_tax_deductions)
        as post_tax_deductions
    , sum(monthly_level_dashboard.total_deductions) as total_deductions
    , sum(monthly_level_dashboard.net_pay) as net_pay
    , sum(monthly_level_dashboard.income_for_reimbursements)
        as income_for_reimbursements
    , sum(monthly_level_dashboard.misc_income) as misc_income
    , sum(monthly_level_dashboard.total_income) as total_income
    , sum(monthly_level_dashboard.needs_spend) as needs_spend
    , sum(monthly_level_dashboard.wants_spend) as wants_spend
    , sum(monthly_level_dashboard.savings_spend) as savings_spend
    , sum(monthly_level_dashboard.emergency_fund_spend)
        as emergency_fund_spend
    , sum(monthly_level_dashboard.savings_saved) as savings_saved
    , sum(monthly_level_dashboard.emergency_fund_saved)
        as emergency_fund_saved
    , sum(monthly_level_dashboard.investments_saved) as investments_saved
    , sum(monthly_level_dashboard.emergency_fund_in_hsa)
        as emergency_fund_in_hsa
    , sum(monthly_level_dashboard.spent) as spent
    , sum(monthly_level_dashboard.difference) as difference
from dashboards.monthly_level as monthly_level_dashboard
group by 1
order by budget_year desc
