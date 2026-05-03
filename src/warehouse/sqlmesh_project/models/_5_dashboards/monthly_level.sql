MODEL (
  name dashboards.monthly_level,
  kind FULL,
  grain budget_month
);

with monthly_ledger as (
    select
        date_trunc('month', ledger_date) as budget_month
        , sum(if(category_group_name_mapping = 'Income', transaction_amount_usd, 0)) as income
        , sum(if(category_name like '%HSA%', -1 * transaction_amount_usd, 0)) as emergency_fund_in_hsa
        , sum(if(category_group_name_mapping = 'Needs', transaction_amount_usd, 0)) as needs_spend
        , sum(if(category_group_name_mapping = 'Wants', transaction_amount_usd, 0)) as wants_spend
        , sum(if(category_group_name_mapping = 'Savings', transaction_amount_usd, 0)) as savings_spend
        , sum(if(category_group_name_mapping = 'Emergency Fund', transaction_amount_usd, 0)) as emergency_fund_spend
        , sum(coalesce(earnings_actual, 0)) as earnings_actual
        , sum(coalesce(salary, 0)) as salary
        , sum(coalesce(bonus, 0)) as bonus
        , sum(coalesce(pre_tax_deductions, 0)) as pre_tax_deductions
        , sum(coalesce(retirement_fund, 0)) as retirement_fund
        , sum(coalesce(hsa, 0)) as hsa
        , sum(coalesce(taxes, 0)) as taxes
        , sum(coalesce(post_tax_deductions, 0)) as post_tax_deductions
        , sum(coalesce(deductions, 0)) as total_deductions
        , sum(coalesce(net_pay, 0)) as net_pay
        , sum(coalesce(income_for_reimbursements, 0)) as income_for_reimbursements
    from core.daily_ledger
    group by 1
)

, monthly_date_spine as (
    select distinct budget_month
    from combined.record_spine
)

select
    monthly_date_spine.budget_month
    , coalesce(monthly_ledger.earnings_actual, 0)::decimal as earnings_actual
    , coalesce(monthly_ledger.salary, 0)::decimal as salary
    , coalesce(monthly_ledger.bonus, 0)::decimal as bonus
    , coalesce(monthly_ledger.pre_tax_deductions, 0)::decimal as pre_tax_deductions
    , coalesce(monthly_ledger.taxes, 0)::decimal as taxes
    , coalesce(monthly_ledger.retirement_fund, 0)::decimal as retirement_fund
    , coalesce(monthly_ledger.hsa, 0)::decimal as hsa
    , coalesce(monthly_ledger.post_tax_deductions, 0)::decimal as post_tax_deductions
    , coalesce(monthly_ledger.total_deductions, 0)::decimal as total_deductions
    , coalesce(monthly_ledger.net_pay, 0)::decimal as net_pay
    , coalesce(monthly_ledger.income_for_reimbursements, 0)::decimal as income_for_reimbursements
    , coalesce(coalesce(monthly_ledger.income, 0)::decimal - coalesce(monthly_ledger.net_pay, 0)::decimal, 0)::decimal as misc_income
    , coalesce(monthly_ledger.income, 0)::decimal as total_income
    , coalesce(monthly_ledger.needs_spend, 0)::decimal as needs_spend
    , coalesce(monthly_ledger.wants_spend, 0)::decimal as wants_spend
    , coalesce(monthly_ledger.savings_spend, 0)::decimal as savings_spend
    , coalesce(monthly_ledger.emergency_fund_spend, 0)::decimal as emergency_fund_spend
    , coalesce(monthly_budgeted.savings_assigned, 0)::decimal as savings_saved
    , coalesce(monthly_budgeted.emergency_fund_assigned, 0)::decimal as emergency_fund_saved
    , coalesce(monthly_budgeted.investments_assigned, 0)::decimal as investments_saved
    , coalesce(monthly_ledger.emergency_fund_in_hsa, 0)::decimal as emergency_fund_in_hsa
    , coalesce(
        monthly_ledger.needs_spend
        + monthly_ledger.wants_spend
        + monthly_ledger.savings_spend
        + monthly_ledger.emergency_fund_spend,
        0
    )::decimal as spent
    , round(
        coalesce(monthly_ledger.income, 0)
        + coalesce(
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
