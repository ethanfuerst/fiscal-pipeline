MODEL (
  name core.monthly_adherance_inputs,
  kind FULL,
  grain budget_month,
  description 'Monthly positive-money inputs for 50/30/15/5 adherence. This model aggregates core ledger and budgeted data without dashboard signs, then calculates monthly targets and rollover inputs.'
);

with monthly_ledger as (
    select
        date_trunc('month', ledger_date) as budget_month  -- First day of the budget month
        , sum(if(category_group_name_mapping = 'Income', coalesce(transaction_inflow_usd, 0) - coalesce(transaction_outflow_usd, 0), 0)) as net_income_to_account  -- Net income deposited to budget accounts, positive USD
        , sum(if(category_group_name_mapping = 'Needs', coalesce(transaction_outflow_usd, 0) - coalesce(transaction_inflow_usd, 0), 0)) as needs_spend  -- Needs net spend, positive USD
        , sum(if(category_group_name_mapping = 'Wants', coalesce(transaction_outflow_usd, 0) - coalesce(transaction_inflow_usd, 0), 0)) as wants_spend  -- Wants net spend, positive USD
        , sum(if(category_group_name_mapping = 'Savings', coalesce(transaction_outflow_usd, 0) - coalesce(transaction_inflow_usd, 0), 0)) as savings_spend  -- Savings net spend, positive USD
        , sum(if(category_group_name_mapping = 'Emergency Fund', coalesce(transaction_outflow_usd, 0) - coalesce(transaction_inflow_usd, 0), 0)) as emergency_fund_spend  -- Emergency Fund net spend, positive USD
        , sum(coalesce(earnings_actual, 0)) as pre_tax_earnings  -- Gross paycheck earnings, positive USD
        , sum(coalesce(pre_tax_deductions_spend, 0)) as pre_tax_deductions_spend  -- Pre-tax FSA/medical deductions, positive USD spend
        , sum(coalesce(taxes_spend, 0)) as taxes_spend  -- Paycheck taxes, positive USD spend
        , sum(coalesce(post_tax_deductions_spend, 0)) as post_tax_deductions_spend  -- Post-tax insurance deductions, positive USD spend
        , sum(coalesce(retirement_fund_saved, 0)) as payroll_retirement_saved  -- Retirement contributions through payroll, positive USD saved
        , sum(coalesce(hsa_saved, 0)) as payroll_hsa_saved  -- HSA contributions through payroll, positive USD saved
        , sum(coalesce(net_pay, 0)) as net_pay  -- Net pay excluding reimbursements, positive USD
    from core.daily_ledger
    group by 1
)

, monthly_date_spine as (
    select distinct budget_month
    from combined.record_spine
)

, normalized_monthly as (
    select
        monthly_date_spine.budget_month  -- First day of the budget month
        , coalesce(monthly_ledger.pre_tax_earnings, 0)::decimal as pre_tax_earnings  -- Gross paycheck earnings, positive USD
        , (
            coalesce(monthly_ledger.pre_tax_deductions_spend, 0)
            + coalesce(monthly_ledger.taxes_spend, 0)
            + coalesce(monthly_ledger.post_tax_deductions_spend, 0)
        )::decimal as taxes_and_deductions_spend  -- Taxes, insurance, FSA, and medical deductions, positive USD spend
        , coalesce(monthly_ledger.net_income_to_account, 0)::decimal as net_income_to_account  -- Net income deposited to budget accounts, positive USD
        , coalesce(monthly_ledger.net_income_to_account, 0)::decimal as plan_income  -- Income basis for 50/30/15/5 from net income deposited to budget accounts, positive USD
        , coalesce(monthly_ledger.payroll_retirement_saved, 0)::decimal as payroll_retirement_saved  -- Retirement contributions through payroll, positive USD saved
        , coalesce(monthly_ledger.payroll_hsa_saved, 0)::decimal as payroll_hsa_saved  -- HSA contributions through payroll, positive USD saved
        , coalesce(monthly_budgeted.investments_saved, 0)::decimal as budgeted_investments_saved  -- Budgeted taxable investments, positive USD saved
        , coalesce(monthly_budgeted.savings_saved, 0)::decimal as budgeted_savings_saved  -- Budgeted Savings category assignments, positive USD saved
        , coalesce(monthly_budgeted.emergency_fund_saved, 0)::decimal as budgeted_emergency_fund_saved  -- Budgeted Emergency Fund category assignments, positive USD saved
        , coalesce(monthly_budgeted.emergency_fund_balance, 0)::decimal as emergency_fund_balance  -- Emergency Fund category balance at month end, positive USD
        , (coalesce(monthly_budgeted.savings_balance, 0) + coalesce(monthly_budgeted.emergency_fund_balance, 0))::decimal as savings_rolling_balance_saved  -- Savings and Emergency Fund balance at month end, USD
        , coalesce(monthly_ledger.needs_spend, 0)::decimal as needs_spend  -- Needs net spend, positive USD
        , coalesce(monthly_ledger.wants_spend, 0)::decimal as wants_spend  -- Wants net spend, positive USD
        , coalesce(monthly_ledger.savings_spend, 0)::decimal as savings_spend  -- Savings net spend, positive USD
        , coalesce(monthly_ledger.emergency_fund_spend, 0)::decimal as emergency_fund_spend  -- Emergency Fund net spend, positive USD
    from monthly_date_spine
    left join monthly_ledger
        on monthly_date_spine.budget_month = monthly_ledger.budget_month
    left join core.monthly_budgeted as monthly_budgeted
        on monthly_date_spine.budget_month = monthly_budgeted.budget_month
)

, targets_and_actuals as (
    select
        *
        , round(plan_income * 0.50, 2)::decimal as target_needs_spend  -- 50% Needs spend target, positive USD
        , round(plan_income * 0.30, 2)::decimal as target_wants_spend  -- 30% Wants spend target, positive USD
        , round(plan_income * 0.15, 2)::decimal as target_investments_saved  -- 15% Investments savings target, positive USD
        , round(plan_income * 0.05, 2)::decimal as target_savings_saved  -- 5% Savings target, positive USD
        , needs_spend as actual_needs_spend  -- Actual Needs spend, positive USD
        , wants_spend as actual_wants_spend  -- Actual Wants spend, positive USD
        , budgeted_investments_saved as actual_investments_saved  -- Budgeted taxable investments, positive USD saved
        , budgeted_savings_saved + budgeted_emergency_fund_saved as savings_before_rollover_saved  -- Budgeted Savings and Emergency Fund assignments, positive USD saved
        , budgeted_savings_saved + budgeted_emergency_fund_saved - savings_spend - emergency_fund_spend as savings_balance_change_saved  -- Savings net after Savings and Emergency Fund spend, positive/negative USD
        , savings_spend + emergency_fund_spend as savings_and_emergency_fund_spend  -- Savings and Emergency Fund net spend, positive USD
    from normalized_monthly
)

select
    budget_month  -- First day of the budget month
    , pre_tax_earnings  -- Gross paycheck earnings, positive USD
    , taxes_and_deductions_spend  -- Taxes, insurance, FSA, and medical deductions, positive USD spend
    , net_income_to_account  -- Net income deposited to budget accounts, positive USD
    , plan_income  -- Income basis for the 50/30/15/5 targets from net income deposited to budget accounts, positive USD
    , payroll_retirement_saved  -- Retirement contributions through payroll, positive USD saved
    , payroll_hsa_saved  -- HSA contributions through payroll, positive USD saved
    , budgeted_investments_saved  -- Budgeted taxable investments, positive USD saved
    , budgeted_savings_saved  -- Budgeted Savings category assignments, positive USD saved
    , budgeted_emergency_fund_saved  -- Budgeted Emergency Fund category assignments, positive USD saved
    , emergency_fund_balance  -- Emergency Fund category balance at month end, positive USD
    , target_needs_spend  -- 50% Needs spend target, positive USD
    , actual_needs_spend  -- Actual Needs spend, positive USD
    , target_wants_spend  -- 30% Wants spend target, positive USD
    , actual_wants_spend  -- Actual Wants spend, positive USD
    , target_investments_saved  -- 15% Investments savings target, positive USD
    , actual_investments_saved  -- Budgeted taxable investments, positive USD saved
    , target_savings_saved  -- 5% Savings target, positive USD
    , savings_before_rollover_saved  -- Budgeted Savings and Emergency Fund assignments, positive USD saved
    , savings_balance_change_saved  -- Savings net after Savings and Emergency Fund spend, positive/negative USD
    , savings_rolling_balance_saved  -- Savings and Emergency Fund balance at month end, USD
    , savings_and_emergency_fund_spend  -- Savings and Emergency Fund net spend, positive USD
    , net_income_to_account
        - actual_needs_spend
        - actual_wants_spend
        - budgeted_savings_saved
        - budgeted_emergency_fund_saved
        - budgeted_investments_saved as actual_rollover  -- Net-to-account income left after actual budget-account spend and budgeted saved amounts
from targets_and_actuals
order by budget_month desc
