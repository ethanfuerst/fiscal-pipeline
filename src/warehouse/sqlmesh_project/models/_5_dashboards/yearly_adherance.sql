MODEL (
  name dashboards.yearly_adherance,
  kind FULL,
  grain budget_year,
  description 'Yearly 50/30/15/5 adherence and runway dashboard. Yearly money columns are summed from monthly adherence; runway columns use the latest monthly runway value in each year.'
);

with monthly_adherance as (
    select
        *
        , extract('year' from budget_month) as budget_year
    from core.monthly_adherance
)

, yearly_sums as (
    select
        budget_year  -- Calendar budget year
        , sum(pre_tax_earnings) as pre_tax_earnings  -- Gross paycheck earnings, positive USD
        , sum(taxes_and_deductions_spend) as taxes_and_deductions_spend  -- Taxes, insurance, FSA, and medical deductions, positive USD spend
        , sum(net_income_to_account) as net_income_to_account  -- Net income deposited to budget accounts, positive USD
        , sum(plan_income) as plan_income  -- Income basis for the 50/30/15/5 targets from net income deposited to budget accounts, positive USD
        , sum(payroll_retirement_saved) as payroll_retirement_saved  -- Retirement contributions through payroll, positive USD saved
        , sum(payroll_hsa_saved) as payroll_hsa_saved  -- HSA contributions through payroll, positive USD saved
        , sum(budgeted_investments_saved) as budgeted_investments_saved  -- Budgeted taxable investments, positive USD saved
        , sum(budgeted_savings_saved) as budgeted_savings_saved  -- Budgeted Savings category assignments, positive USD saved
        , sum(budgeted_emergency_fund_saved) as budgeted_emergency_fund_saved  -- Budgeted Emergency Fund category assignments, positive USD saved
        , sum(target_needs_spend) as target_needs_spend  -- 50% Needs spend target, positive USD
        , sum(actual_needs_spend) as actual_needs_spend  -- Actual Needs spend, positive USD
        , sum(target_wants_spend) as target_wants_spend  -- 30% Wants spend target, positive USD
        , sum(actual_wants_spend) as actual_wants_spend  -- Actual Wants spend, positive USD
        , sum(target_investments_saved) as target_investments_saved  -- 15% Investments savings target, positive USD
        , sum(actual_investments_saved) as actual_investments_saved  -- Budgeted taxable investments, positive USD saved
        , sum(target_savings_saved) as target_savings_saved  -- 5% Savings target, positive USD
        , sum(savings_before_rollover_saved) as savings_before_rollover_saved  -- Budgeted Savings and Emergency Fund assignments, positive USD saved
        , sum(rollover_to_next_month_saved) as rollover_to_next_month_saved  -- Positive monthly rollover, positive USD
        , sum(actual_savings_saved) as actual_savings_saved  -- Budgeted Savings and Emergency Fund assignments, positive USD saved
        , sum(savings_balance_change_saved) as savings_balance_change_saved  -- Savings net after Savings and Emergency Fund spend, positive/negative USD
        , sum(savings_and_emergency_fund_spend) as savings_and_emergency_fund_spend  -- Savings and Emergency Fund net spend, positive USD
        , sum(actual_rollover) as actual_rollover  -- Net-to-account income left after actual budget-account spend and budgeted saved amounts
        , avg(monthly_cash_spend) as monthly_cash_spend  -- Average monthly Needs and Wants cash spend in the year, positive USD
    from monthly_adherance
    group by 1
)

, latest_monthly_runway as (
    select
        budget_year
        , budget_month as runway_budget_month  -- Latest month represented in the yearly runway metrics
        , emergency_fund_balance  -- Latest Emergency Fund category balance in the year, positive USD
        , savings_rolling_balance_saved  -- Latest Savings and Emergency Fund balance in the year, USD
        , avg_monthly_cash_spend_3mo  -- Latest three-month trailing average Needs and Wants cash spend in the year, positive USD
        , runway_months  -- Latest runway months in the year
        , total_runway_months  -- Latest total runway months in the year
        , runway_months_shortfall  -- Latest runway months shortfall in the year
        , total_runway_months_shortfall  -- Latest total runway months shortfall in the year
        , has_target_runway  -- Latest one-month runway target flag in the year
        , has_target_total_runway  -- Latest 3.5-month total runway target flag in the year
    from (
        select
            *
            , row_number() over (partition by budget_year order by budget_month desc) as month_rank
        from monthly_adherance
    )
    where month_rank = 1
)

, yearly_adherance as (
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
    from yearly_sums
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
            - budgeted_investments_saved as planned_rollover_before_spend_overage  -- Rollover before charging Needs/Wants overages against the year, positive/negative USD
    from yearly_adherance
)

, rollover_usage as (
    select
        *
        , least(greatest(planned_rollover_before_spend_overage, 0), needs_spend_over_target)::decimal as rollover_used_for_needs_overage  -- Yearly rollover consumed by Needs overspend, positive USD
        , planned_rollover_before_spend_overage - needs_spend_over_target as rollover_after_needs_overage  -- Rollover remaining after Needs overspend is charged, positive/negative USD
    from rollover_waterfall
)

, final as (
    select
        rollover_usage.*
        , least(greatest(rollover_after_needs_overage, 0), wants_spend_over_target)::decimal as rollover_used_for_wants_overage  -- Yearly rollover consumed by Wants overspend, positive USD
        , planned_rollover_before_spend_overage - spending_over_target as rollover_after_spend_overages  -- Rollover after Needs and Wants overages are charged, positive/negative USD
        , greatest((planned_rollover_before_spend_overage - spending_over_target) * -1, 0)::decimal as rollover_shortfall_after_spend_overages  -- Amount by which spending overages exceed available yearly rollover, positive USD
        , actual_needs_spend <= target_needs_spend as needs_on_plan  -- True when Needs spend is at or below the 50% target
        , actual_wants_spend <= target_wants_spend as wants_on_plan  -- True when Wants spend is at or below the 30% target
        , actual_investments_saved >= target_investments_saved as investments_on_plan  -- True when Investments savings meet or exceed the 15% target
        , actual_savings_saved >= target_savings_saved as savings_on_plan  -- True when Savings meet or exceed the 5% target
    from rollover_usage
)

select
    final.budget_year  -- Calendar budget year
    , final.pre_tax_earnings  -- Gross paycheck earnings, positive USD
    , final.taxes_and_deductions_spend  -- Taxes, insurance, FSA, and medical deductions, positive USD spend
    , final.net_income_to_account  -- Net income deposited to budget accounts, positive USD
    , final.plan_income  -- Income basis for the 50/30/15/5 targets from net income deposited to budget accounts, positive USD
    , final.payroll_retirement_saved  -- Retirement contributions through payroll, positive USD saved
    , final.payroll_hsa_saved  -- HSA contributions through payroll, positive USD saved
    , final.budgeted_investments_saved  -- Budgeted taxable investments, positive USD saved
    , final.budgeted_savings_saved  -- Budgeted Savings category assignments, positive USD saved
    , final.budgeted_emergency_fund_saved  -- Budgeted Emergency Fund category assignments, positive USD saved
    , final.target_needs_spend  -- 50% Needs spend target, positive USD
    , final.actual_needs_spend  -- Actual Needs spend, positive USD
    , final.needs_spend_over_target  -- Needs overspend above target, positive USD
    , final.needs_spend_under_target  -- Needs room below target, positive USD
    , final.target_wants_spend  -- 30% Wants spend target, positive USD
    , final.actual_wants_spend  -- Actual Wants spend, positive USD
    , final.wants_spend_over_target  -- Wants overspend above target, positive USD
    , final.wants_spend_under_target  -- Wants room below target, positive USD
    , final.target_investments_saved  -- 15% Investments savings target, positive USD
    , final.actual_investments_saved  -- Budgeted taxable investments, positive USD saved
    , final.investments_saved_shortfall  -- Amount needed to hit the Investments target, positive USD
    , final.investments_saved_over_target  -- Investments savings above target, positive USD
    , final.target_savings_saved  -- 5% Savings target, positive USD
    , final.savings_before_rollover_saved  -- Budgeted Savings and Emergency Fund assignments, positive USD saved
    , final.rollover_to_next_month_saved  -- Positive monthly rollover, positive USD
    , final.actual_savings_saved  -- Budgeted Savings and Emergency Fund assignments, positive USD saved
    , final.savings_balance_change_saved  -- Savings net after Savings and Emergency Fund spend, positive/negative USD
    , final.savings_saved_shortfall  -- Amount needed to hit the Savings target, positive USD
    , final.savings_saved_over_target  -- Savings above target, positive USD
    , final.savings_and_emergency_fund_spend  -- Savings and Emergency Fund net spend, positive USD
    , final.spending_over_target  -- Total Needs and Wants overspend above targets, positive USD
    , final.planned_rollover_before_spend_overage  -- Rollover before charging Needs/Wants overages, positive/negative USD
    , final.rollover_used_for_needs_overage  -- Yearly rollover consumed by Needs overspend, positive USD
    , final.rollover_used_for_wants_overage  -- Yearly rollover consumed by Wants overspend, positive USD
    , final.rollover_after_spend_overages  -- Rollover after Needs and Wants overages are charged, positive/negative USD
    , final.rollover_shortfall_after_spend_overages  -- Spending overage not covered by yearly rollover, positive USD
    , final.actual_rollover  -- Net-to-account income left after actual budget-account spend and budgeted saved amounts
    , final.monthly_cash_spend  -- Average monthly Needs and Wants cash spend in the year, positive USD
    , latest_monthly_runway.runway_budget_month  -- Latest month represented in the yearly runway metrics
    , latest_monthly_runway.emergency_fund_balance  -- Latest Emergency Fund category balance in the year, positive USD
    , latest_monthly_runway.savings_rolling_balance_saved  -- Latest Savings and Emergency Fund balance in the year, USD
    , latest_monthly_runway.avg_monthly_cash_spend_3mo  -- Latest three-month trailing average Needs and Wants cash spend in the year, positive USD
    , latest_monthly_runway.runway_months  -- Latest runway months in the year
    , latest_monthly_runway.total_runway_months  -- Latest total runway months in the year
    , latest_monthly_runway.runway_months_shortfall  -- Latest runway months shortfall in the year
    , latest_monthly_runway.total_runway_months_shortfall  -- Latest total runway months shortfall in the year
    , latest_monthly_runway.has_target_runway  -- Latest one-month runway target flag in the year
    , latest_monthly_runway.has_target_total_runway  -- Latest 3.5-month total runway target flag in the year
    , case when final.target_needs_spend = 0 then null else round(final.actual_needs_spend / final.target_needs_spend, 4) end as needs_target_ratio  -- Needs actual divided by target; values over 1.0 are overspend
    , case when final.target_wants_spend = 0 then null else round(final.actual_wants_spend / final.target_wants_spend, 4) end as wants_target_ratio  -- Wants actual divided by target; values over 1.0 are overspend
    , case when final.target_investments_saved = 0 then null else round(final.actual_investments_saved / final.target_investments_saved, 4) end as investments_target_ratio  -- Investments actual divided by target; values under 1.0 are shortfall
    , case when final.target_savings_saved = 0 then null else round(final.actual_savings_saved / final.target_savings_saved, 4) end as savings_target_ratio  -- Savings actual divided by target; values under 1.0 are shortfall
    , final.needs_on_plan  -- True when Needs spend is at or below target
    , final.wants_on_plan  -- True when Wants spend is at or below target
    , final.investments_on_plan  -- True when Investments savings meet or exceed target
    , final.savings_on_plan  -- True when Savings meet or exceed target
    , case when final.needs_on_plan then 1 else 0 end
        + case when final.wants_on_plan then 1 else 0 end
        + case when final.investments_on_plan then 1 else 0 end
        + case when final.savings_on_plan then 1 else 0 end as buckets_on_plan_count  -- Count of 50/30/15/5 buckets on plan, from 0 to 4
    , final.needs_on_plan and final.wants_on_plan and final.investments_on_plan and final.savings_on_plan as overall_on_plan  -- True when all 50/30/15/5 buckets are on plan
from final
left join latest_monthly_runway
    on final.budget_year = latest_monthly_runway.budget_year
order by final.budget_year desc
