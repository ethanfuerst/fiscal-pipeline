MODEL (
  name dashboards.monthly_level,
  kind FULL,
  grain budget_month,
  audits (
    monthly_needs_wants_match_transactions
  ),
  description 'Monthly dashboard overview. This layer preserves the legacy signed dashboard contract: deduction/spend columns are negative, income/saved columns are positive.'
);

with monthly_ledger as (
    select
        date_trunc('month', ledger_date) as budget_month  -- First day of the budget month
        , sum(if(category_group_name_mapping = 'Income', coalesce(transaction_inflow_usd, 0) - coalesce(transaction_outflow_usd, 0), 0)) as income  -- Net income-category ledger activity, positive for net inflow
        , sum(if(category_group_name_mapping = 'Needs', coalesce(transaction_outflow_usd, 0) - coalesce(transaction_inflow_usd, 0), 0)) as needs_spend  -- Needs net spend, positive USD
        , sum(if(category_group_name_mapping = 'Wants', coalesce(transaction_outflow_usd, 0) - coalesce(transaction_inflow_usd, 0), 0)) as wants_spend  -- Wants net spend, positive USD
        , sum(if(category_group_name_mapping = 'Savings', coalesce(transaction_outflow_usd, 0) - coalesce(transaction_inflow_usd, 0), 0)) as savings_spend  -- Savings net spend, positive USD
        , sum(if(category_group_name_mapping = 'Emergency Fund', coalesce(transaction_outflow_usd, 0) - coalesce(transaction_inflow_usd, 0), 0)) as emergency_fund_spend  -- Emergency Fund net spend, positive USD
        , sum(coalesce(earnings_actual, 0)) as earnings_actual  -- Gross earnings, positive USD
        , sum(coalesce(salary, 0)) as salary  -- Salary earnings, positive USD
        , sum(coalesce(bonus, 0)) as bonus  -- Bonus/PTO/severance earnings, positive USD
        , sum(coalesce(pre_tax_deductions_spend, 0)) as pre_tax_deductions_spend  -- Pre-tax FSA/medical deductions, positive USD spend
        , sum(coalesce(retirement_fund_saved, 0)) as retirement_fund_saved  -- Retirement contributions, positive USD saved
        , sum(coalesce(hsa_saved, 0)) as hsa_saved  -- HSA contributions, positive USD saved
        , sum(coalesce(taxes_spend, 0)) as taxes_spend  -- Paycheck taxes, positive USD spend
        , sum(coalesce(post_tax_deductions_spend, 0)) as post_tax_deductions_spend  -- Post-tax insurance deductions, positive USD spend
        , sum(coalesce(paycheck_withheld, 0)) as paycheck_withheld  -- Total paycheck withholdings, positive USD
        , sum(coalesce(net_pay, 0)) as net_pay  -- Net pay excluding reimbursements, positive USD
        , sum(coalesce(reimbursement_income, 0)) as reimbursement_income  -- Non-taxable reimbursement income, positive USD
    from core.daily_ledger
    group by 1
)

, monthly_date_spine as (
    select distinct budget_month
    from combined.record_spine
)

select
    monthly_date_spine.budget_month  -- First day of the budget month
    , coalesce(monthly_ledger.earnings_actual, 0)::decimal as earnings_actual  -- Gross earnings, positive USD
    , coalesce(monthly_ledger.salary, 0)::decimal as salary  -- Salary earnings, positive USD
    , coalesce(monthly_ledger.bonus, 0)::decimal as bonus  -- Bonus/PTO/severance earnings, positive USD
    , -1 * coalesce(monthly_ledger.pre_tax_deductions_spend, 0)::decimal as pre_tax_deductions  -- Pre-tax FSA/medical deductions, signed negative for dashboard display
    , -1 * coalesce(monthly_ledger.taxes_spend, 0)::decimal as taxes  -- Paycheck taxes, signed negative for dashboard display
    , -1 * coalesce(monthly_ledger.retirement_fund_saved, 0)::decimal as retirement_fund  -- Retirement contributions, signed negative for dashboard display
    , -1 * coalesce(monthly_ledger.hsa_saved, 0)::decimal as hsa  -- HSA contributions, signed negative for dashboard display
    , -1 * coalesce(monthly_ledger.post_tax_deductions_spend, 0)::decimal as post_tax_deductions  -- Post-tax insurance deductions, signed negative for dashboard display
    , -1 * coalesce(monthly_ledger.paycheck_withheld, 0)::decimal as total_deductions  -- Total paycheck withholdings, signed negative for dashboard display
    , coalesce(monthly_ledger.net_pay, 0)::decimal as net_pay  -- Net pay excluding reimbursements, positive USD
    , coalesce(monthly_ledger.reimbursement_income, 0)::decimal as income_for_reimbursements  -- Non-taxable reimbursement income, positive USD
    , coalesce(coalesce(monthly_ledger.income, 0)::decimal - coalesce(monthly_ledger.net_pay, 0)::decimal, 0)::decimal as misc_income  -- Income-category inflow not explained by net pay, positive/negative USD
    , coalesce(monthly_ledger.income, 0)::decimal as total_income  -- Net income-category ledger activity, positive for net inflow
    , -1 * coalesce(monthly_ledger.needs_spend, 0)::decimal as needs_spend  -- Needs net spend, signed negative for dashboard display
    , -1 * coalesce(monthly_ledger.wants_spend, 0)::decimal as wants_spend  -- Wants net spend, signed negative for dashboard display
    , -1 * coalesce(monthly_ledger.savings_spend, 0)::decimal as savings_spend  -- Savings net spend, signed negative for dashboard display
    , -1 * coalesce(monthly_ledger.emergency_fund_spend, 0)::decimal as emergency_fund_spend  -- Emergency Fund net spend, signed negative for dashboard display
    , coalesce(monthly_budgeted.savings_saved, 0)::decimal as savings_saved  -- Amount assigned to Savings categories, positive USD
    , coalesce(monthly_budgeted.emergency_fund_saved, 0)::decimal as emergency_fund_saved  -- Amount assigned to Emergency Fund categories, positive USD
    , coalesce(monthly_budgeted.investments_saved, 0)::decimal as investments_saved  -- Amount assigned to Investments categories, positive USD
    , -1 * coalesce(
        monthly_ledger.needs_spend
        + monthly_ledger.wants_spend
        + monthly_ledger.savings_spend
        + monthly_ledger.emergency_fund_spend,
        0
    )::decimal as spent  -- Total Needs/Wants/Savings/Emergency Fund spend, signed negative for dashboard display
    , round(
        coalesce(monthly_ledger.income, 0)
        - coalesce(
            monthly_ledger.needs_spend
            + monthly_ledger.wants_spend
            + monthly_ledger.savings_spend
            + monthly_ledger.emergency_fund_spend,
            0
        ),
        2
    )::decimal as difference
from monthly_date_spine
left join monthly_ledger
    on monthly_date_spine.budget_month = monthly_ledger.budget_month
left join core.monthly_budgeted as monthly_budgeted
    on monthly_date_spine.budget_month = monthly_budgeted.budget_month
order by monthly_date_spine.budget_month desc
