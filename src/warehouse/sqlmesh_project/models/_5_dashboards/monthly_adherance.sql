MODEL (
  name dashboards.monthly_adherance,
  kind FULL,
  grain budget_month,
  description 'Monthly 50/30/15/5 adherence and runway dashboard from core.monthly_adherance.'
);

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
    , needs_spend_over_target  -- Needs overspend above target, positive USD
    , needs_spend_under_target  -- Needs room below target, positive USD
    , target_wants_spend  -- 30% Wants spend target, positive USD
    , actual_wants_spend  -- Actual Wants spend, positive USD
    , wants_spend_over_target  -- Wants overspend above target, positive USD
    , wants_spend_under_target  -- Wants room below target, positive USD
    , target_investments_saved  -- 15% Investments savings target, positive USD
    , actual_investments_saved  -- Budgeted taxable investments, positive USD saved
    , investments_saved_shortfall  -- Amount needed to hit the Investments target, positive USD
    , investments_saved_over_target  -- Investments savings above target, positive USD
    , target_savings_saved  -- 5% Savings target, positive USD
    , savings_before_rollover_saved  -- Budgeted Savings and Emergency Fund assignments, positive USD saved
    , rollover_to_next_month_saved  -- Positive rollover carried to the next month, positive USD
    , actual_savings_saved  -- Budgeted Savings and Emergency Fund assignments, positive USD saved
    , savings_balance_change_saved  -- Savings net after Savings and Emergency Fund spend, positive/negative USD
    , savings_rolling_balance_saved  -- Savings and Emergency Fund balance at month end, USD
    , savings_saved_shortfall  -- Amount needed to hit the Savings target, positive USD
    , savings_saved_over_target  -- Savings above target, positive USD
    , savings_and_emergency_fund_spend  -- Savings and Emergency Fund net spend, positive USD
    , spending_over_target  -- Total Needs and Wants overspend above targets, positive USD
    , planned_rollover_before_spend_overage  -- Rollover before charging Needs/Wants overages, positive/negative USD
    , rollover_used_for_needs_overage  -- Same-month rollover consumed by Needs overspend, positive USD
    , rollover_used_for_wants_overage  -- Same-month rollover consumed by Wants overspend, positive USD
    , rollover_after_spend_overages  -- Rollover after Needs and Wants overages are charged, positive/negative USD
    , rollover_shortfall_after_spend_overages  -- Spending overage not covered by same-month rollover, positive USD
    , actual_rollover  -- Net-to-account income left after actual budget-account spend and budgeted saved amounts
    , monthly_cash_spend  -- Actual Needs and Wants cash spend used for runway, positive USD
    , avg_monthly_cash_spend_3mo  -- Three-month trailing average Needs and Wants cash spend used for runway, positive USD
    , runway_months  -- Months of average spend covered by positive rollover cash
    , total_runway_months  -- Months of average spend covered by rollover cash plus Emergency Fund balance
    , runway_months_shortfall  -- Additional rollover runway months needed to reach the one-month target
    , total_runway_months_shortfall  -- Additional total runway months needed to reach the 3.5-month target
    , has_target_runway  -- True when rollover covers at least one month of average spend
    , has_target_total_runway  -- True when rollover plus Emergency Fund covers at least 3.5 months of average spend
    , needs_target_ratio  -- Needs actual divided by target; values over 1.0 are overspend
    , wants_target_ratio  -- Wants actual divided by target; values over 1.0 are overspend
    , investments_target_ratio  -- Investments actual divided by target; values under 1.0 are shortfall
    , savings_target_ratio  -- Savings actual divided by target; values under 1.0 are shortfall
    , needs_on_plan  -- True when Needs spend is at or below target
    , wants_on_plan  -- True when Wants spend is at or below target
    , investments_on_plan  -- True when Investments savings meet or exceed target
    , savings_on_plan  -- True when Savings meet or exceed target
    , buckets_on_plan_count  -- Count of 50/30/15/5 buckets on plan, from 0 to 4
    , overall_on_plan  -- True when all 50/30/15/5 buckets are on plan
from core.monthly_adherance
order by budget_month desc
