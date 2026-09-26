MODEL (
  name core.monthly_adherance,
  kind FULL,
  grain budget_month,
  description 'Monthly 50/30/15/5 adherence and runway calculations from core.monthly_adherance_inputs.'
);

with savings_actuals as (
    select
        *
        , greatest(actual_rollover, 0)::decimal as rollover_to_next_month_saved  -- Positive rollover carried to the next month, positive USD
        , savings_before_rollover_saved as actual_savings_saved  -- Budgeted Savings and Emergency Fund assignments, positive USD saved
    from core.monthly_adherance_inputs
)

, adherence_amounts as (
    select
        *
        , greatest(actual_needs_spend - target_needs_spend, 0)::decimal as needs_spend_over_target  -- Needs overspend above the 50% target, positive USD
        , greatest(target_needs_spend - actual_needs_spend, 0)::decimal as needs_spend_under_target  -- Needs room below the 50% target, positive USD
        , greatest(actual_wants_spend - target_wants_spend, 0)::decimal as wants_spend_over_target  -- Wants overspend above the 30% target, positive USD
        , greatest(target_wants_spend - actual_wants_spend, 0)::decimal as wants_spend_under_target  -- Wants room below the 30% target, positive USD
        , greatest(target_investments_saved - actual_investments_saved, 0)::decimal as investments_saved_shortfall  -- Amount needed to hit the 15% Investments target, positive USD
        , greatest(actual_investments_saved - target_investments_saved, 0)::decimal as investments_saved_over_target  -- Investments savings above the 15% target, positive USD
        , greatest(target_savings_saved - actual_savings_saved, 0)::decimal as savings_saved_shortfall  -- Amount needed to hit the 5% Savings target, positive USD
        , greatest(actual_savings_saved - target_savings_saved, 0)::decimal as savings_saved_over_target  -- Savings above the 5% target, positive USD
    from savings_actuals
)

, rollover_waterfall as (
    select
        *
        , needs_spend_over_target + wants_spend_over_target as spending_over_target  -- Total Needs and Wants overspend above 50/30 targets, positive USD
        , net_income_to_account
            - least(actual_needs_spend, target_needs_spend)
            - least(actual_wants_spend, target_wants_spend)
            - budgeted_savings_saved
            - budgeted_emergency_fund_saved
            - budgeted_investments_saved as planned_rollover_before_spend_overage  -- Rollover before charging Needs/Wants overages against the month, positive/negative USD
    from adherence_amounts
)

, rollover_usage as (
    select
        *
        , least(greatest(planned_rollover_before_spend_overage, 0), needs_spend_over_target)::decimal as rollover_used_for_needs_overage  -- Same-month rollover consumed by Needs overspend, positive USD
        , planned_rollover_before_spend_overage - needs_spend_over_target as rollover_after_needs_overage  -- Rollover remaining after Needs overspend is charged, positive/negative USD
    from rollover_waterfall
)

, adherence_summary as (
    select
        *
        , least(greatest(rollover_after_needs_overage, 0), wants_spend_over_target)::decimal as rollover_used_for_wants_overage  -- Same-month rollover consumed by Wants overspend, positive USD
        , planned_rollover_before_spend_overage - spending_over_target as rollover_after_spend_overages  -- Rollover after Needs and Wants overages are charged, positive/negative USD
        , greatest((planned_rollover_before_spend_overage - spending_over_target) * -1, 0)::decimal as rollover_shortfall_after_spend_overages  -- Amount by which spending overages exceed available same-month rollover, positive USD
        , actual_needs_spend <= target_needs_spend as needs_on_plan  -- True when Needs spend is at or below the 50% target
        , actual_wants_spend <= target_wants_spend as wants_on_plan  -- True when Wants spend is at or below the 30% target
        , actual_investments_saved >= target_investments_saved as investments_on_plan  -- True when Investments savings meet or exceed the 15% target
        , actual_savings_saved >= target_savings_saved as savings_on_plan  -- True when Savings meet or exceed the 5% target
    from rollover_usage
)

, runway_metrics as (
    select
        *
        , actual_needs_spend + actual_wants_spend as monthly_cash_spend  -- Actual Needs and Wants cash spend used for runway, positive USD
        , avg(actual_needs_spend + actual_wants_spend)
            over (order by budget_month rows between 2 preceding and current row) as avg_monthly_cash_spend_3mo  -- Three-month trailing average Needs and Wants cash spend used for runway, positive USD
    from adherence_summary
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
    , case when avg_monthly_cash_spend_3mo = 0 then null else round(rollover_to_next_month_saved / avg_monthly_cash_spend_3mo, 4) end as runway_months  -- Months of average spend covered by positive rollover cash
    , case when avg_monthly_cash_spend_3mo = 0 then null else round((rollover_to_next_month_saved + emergency_fund_balance) / avg_monthly_cash_spend_3mo, 4) end as total_runway_months  -- Months of average spend covered by rollover cash plus Emergency Fund balance
    , case when avg_monthly_cash_spend_3mo = 0 then null else greatest(1.00 - round(rollover_to_next_month_saved / avg_monthly_cash_spend_3mo, 4), 0) end as runway_months_shortfall  -- Additional rollover runway months needed to reach the one-month target
    , case when avg_monthly_cash_spend_3mo = 0 then null else greatest(3.50 - round((rollover_to_next_month_saved + emergency_fund_balance) / avg_monthly_cash_spend_3mo, 4), 0) end as total_runway_months_shortfall  -- Additional total runway months needed to reach the 3.5-month target
    , case when avg_monthly_cash_spend_3mo = 0 then false else rollover_to_next_month_saved / avg_monthly_cash_spend_3mo >= 1.00 end as has_target_runway  -- True when rollover covers at least one month of average spend
    , case when avg_monthly_cash_spend_3mo = 0 then false else (rollover_to_next_month_saved + emergency_fund_balance) / avg_monthly_cash_spend_3mo >= 3.50 end as has_target_total_runway  -- True when rollover plus Emergency Fund covers at least 3.5 months of average spend
    , case when target_needs_spend = 0 then null else round(actual_needs_spend / target_needs_spend, 4) end as needs_target_ratio  -- Needs actual divided by target; values over 1.0 are overspend
    , case when target_wants_spend = 0 then null else round(actual_wants_spend / target_wants_spend, 4) end as wants_target_ratio  -- Wants actual divided by target; values over 1.0 are overspend
    , case when target_investments_saved = 0 then null else round(actual_investments_saved / target_investments_saved, 4) end as investments_target_ratio  -- Investments actual divided by target; values under 1.0 are shortfall
    , case when target_savings_saved = 0 then null else round(actual_savings_saved / target_savings_saved, 4) end as savings_target_ratio  -- Savings actual divided by target; values under 1.0 are shortfall
    , needs_on_plan  -- True when Needs spend is at or below target
    , wants_on_plan  -- True when Wants spend is at or below target
    , investments_on_plan  -- True when Investments savings meet or exceed target
    , savings_on_plan  -- True when Savings meet or exceed target
    , case when needs_on_plan then 1 else 0 end
        + case when wants_on_plan then 1 else 0 end
        + case when investments_on_plan then 1 else 0 end
        + case when savings_on_plan then 1 else 0 end as buckets_on_plan_count  -- Count of 50/30/15/5 buckets on plan, from 0 to 4
    , needs_on_plan and wants_on_plan and investments_on_plan and savings_on_plan as overall_on_plan  -- True when all 50/30/15/5 buckets are on plan
from runway_metrics
order by budget_month desc
