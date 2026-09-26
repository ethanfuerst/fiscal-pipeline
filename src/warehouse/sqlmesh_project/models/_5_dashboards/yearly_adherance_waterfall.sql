MODEL (
  name dashboards.yearly_adherance_waterfall,
  kind FULL,
  grain budget_year,
  description 'Yearly 50/30/15/5 waterfall showing annual category targets, overflow dollars, latest buffer state, and latest emergency fund state.'
);

with yearly_base as (
    select
        budget_year
        , net_income_to_account
        , plan_income
        , target_needs_spend
        , actual_needs_spend
        , needs_spend_over_target
        , needs_spend_under_target
        , target_wants_spend
        , actual_wants_spend
        , wants_spend_over_target
        , wants_spend_under_target
        , target_savings_saved
        , actual_savings_saved
        , savings_balance_change_saved
        , savings_rolling_balance_saved
        , savings_saved_shortfall
        , savings_saved_over_target
        , target_investments_saved
        , actual_investments_saved
        , investments_saved_shortfall
        , investments_saved_over_target
        , actual_rollover
        , monthly_cash_spend
    from dashboards.yearly_adherance
)

, monthly_waterfall_with_year as (
    select
        extract('year' from budget_month) as budget_year
        , budget_month
        , overflow_dollars_saved
        , runway_cash_saved
        , buffer_balance_saved
        , buffer_target_saved
        , buffer_gap_saved
        , buffer_surplus_saved
        , overflow_to_buffer_saved
        , used_from_buffer_saved
        , uncovered_shortfall_spend
        , true_excess_saved
        , emergency_fund_balance
        , emergency_fund_target_saved
        , emergency_fund_gap_saved
        , emergency_fund_surplus_saved
        , reserve_surplus_saved
        , monthly_cash_spend
        , avg_complete_monthly_cash_spend_3mo
        , target_total_runway_cash_saved
        , required_rollover_cash_saved
        , rollover_runway_months
        , total_runway_months
        , rollover_cash_shortfall_saved
        , excess_rollover_cash_saved
        , has_balanced_runway
    from dashboards.monthly_adherance_waterfall
)

, yearly_overflow as (
    select
        budget_year
        , sum(overflow_dollars_saved)::decimal as overflow_dollars_saved  -- Annual sum of monthly overflow dollars
    from monthly_waterfall_with_year
    group by budget_year
)

/*
    Yearly buffer metrics are snapshots, not annual totals. The monthly model owns the Buffer Balance identity:
    Needs/Wants available plus income received but not assigned through that month, excluding Credit Card Payments assignments.

    This CTE selects the last monthly row that is valid for each year. Completed years naturally use their final emitted month,
    while the current year uses the current month and ignores any future-budgeted rows. That keeps yearly Buffer Balance,
    Buffer Surplus, and Reserve Surplus aligned with the monthly state that would have been knowable at that point in time.
*/
, latest_monthly_runway as (
    select
        budget_year
        , budget_month as runway_budget_month
        , runway_cash_saved
        , buffer_balance_saved
        , buffer_target_saved
        , buffer_gap_saved
        , buffer_surplus_saved
        , overflow_to_buffer_saved
        , used_from_buffer_saved
        , uncovered_shortfall_spend
        , true_excess_saved
        , emergency_fund_balance
        , emergency_fund_target_saved
        , emergency_fund_gap_saved
        , emergency_fund_surplus_saved
        , reserve_surplus_saved
        , monthly_cash_spend
        , avg_complete_monthly_cash_spend_3mo
        , target_total_runway_cash_saved
        , required_rollover_cash_saved
        , rollover_runway_months
        , total_runway_months
        , rollover_cash_shortfall_saved
        , excess_rollover_cash_saved
        , has_balanced_runway
    from (
        select
            budget_year
            , budget_month
            , runway_cash_saved
            , buffer_balance_saved
            , buffer_target_saved
            , buffer_gap_saved
            , buffer_surplus_saved
            , overflow_to_buffer_saved
            , used_from_buffer_saved
            , uncovered_shortfall_spend
            , true_excess_saved
            , emergency_fund_balance
            , emergency_fund_target_saved
            , emergency_fund_gap_saved
            , emergency_fund_surplus_saved
            , reserve_surplus_saved
            , monthly_cash_spend
            , avg_complete_monthly_cash_spend_3mo
            , target_total_runway_cash_saved
            , required_rollover_cash_saved
            , rollover_runway_months
            , total_runway_months
            , rollover_cash_shortfall_saved
            , excess_rollover_cash_saved
            , has_balanced_runway
            , row_number() over (partition by budget_year order by budget_month desc) as month_rank
        from monthly_waterfall_with_year
        where budget_month <= date_trunc('month', current_date)
    )
    where month_rank = 1
)

, yearly_with_runway as (
    select
        yearly_base.budget_year
        , yearly_base.net_income_to_account
        , yearly_base.plan_income
        , yearly_overflow.overflow_dollars_saved
        , yearly_base.target_needs_spend
        , yearly_base.actual_needs_spend
        , yearly_base.needs_spend_over_target
        , yearly_base.needs_spend_under_target
        , yearly_base.target_wants_spend
        , yearly_base.actual_wants_spend
        , yearly_base.wants_spend_over_target
        , yearly_base.wants_spend_under_target
        , yearly_base.target_savings_saved
        , yearly_base.actual_savings_saved
        , yearly_base.savings_balance_change_saved
        , yearly_base.savings_rolling_balance_saved
        , yearly_base.savings_saved_shortfall
        , yearly_base.savings_saved_over_target
        , yearly_base.target_investments_saved
        , yearly_base.actual_investments_saved
        , yearly_base.investments_saved_shortfall
        , yearly_base.investments_saved_over_target
        , yearly_base.actual_rollover
        , yearly_base.monthly_cash_spend
        , latest_monthly_runway.runway_budget_month
        , latest_monthly_runway.runway_cash_saved
        , latest_monthly_runway.buffer_balance_saved
        , latest_monthly_runway.buffer_target_saved
        , latest_monthly_runway.buffer_gap_saved
        , latest_monthly_runway.buffer_surplus_saved
        , latest_monthly_runway.overflow_to_buffer_saved
        , latest_monthly_runway.used_from_buffer_saved
        , latest_monthly_runway.uncovered_shortfall_spend
        , latest_monthly_runway.true_excess_saved
        , latest_monthly_runway.emergency_fund_balance
        , latest_monthly_runway.emergency_fund_target_saved
        , latest_monthly_runway.emergency_fund_gap_saved
        , latest_monthly_runway.emergency_fund_surplus_saved
        , latest_monthly_runway.reserve_surplus_saved
        , latest_monthly_runway.avg_complete_monthly_cash_spend_3mo
        , latest_monthly_runway.target_total_runway_cash_saved
        , latest_monthly_runway.required_rollover_cash_saved
        , latest_monthly_runway.rollover_runway_months
        , latest_monthly_runway.total_runway_months
        , latest_monthly_runway.rollover_cash_shortfall_saved
        , latest_monthly_runway.excess_rollover_cash_saved
        , latest_monthly_runway.has_balanced_runway
    from yearly_base
    left join yearly_overflow
        on yearly_base.budget_year = yearly_overflow.budget_year
    left join latest_monthly_runway
        on yearly_base.budget_year = latest_monthly_runway.budget_year
)

, overflow_state as (
    select
        yearly_with_runway.*
        , (coalesce(savings_rolling_balance_saved, 0) - coalesce(emergency_fund_balance, 0))::decimal as other_savings_balance_saved  -- Latest Savings balance outside the Emergency Fund bucket, USD
    from yearly_with_runway
)

, needs_waterfall as (
    select
        overflow_state.*
        , case when excess_rollover_cash_saved is null then null else least(excess_rollover_cash_saved, needs_spend_over_target)::decimal end as needs_gap_covered_saved  -- Excess cash used to cover annual Needs overspend before lower-priority gaps, positive USD
        , case when excess_rollover_cash_saved is null then null else greatest(needs_spend_over_target - excess_rollover_cash_saved, 0)::decimal end as needs_gap_uncovered_spend  -- Annual Needs overspend still uncovered after excess cash, positive USD spend
        , case when excess_rollover_cash_saved is null then null else greatest(excess_rollover_cash_saved - needs_spend_over_target, 0)::decimal end as excess_cash_after_needs_saved  -- Excess cash remaining after annual Needs gap coverage, positive USD
    from overflow_state
)

, wants_waterfall as (
    select
        needs_waterfall.*
        , case when excess_cash_after_needs_saved is null then null else least(excess_cash_after_needs_saved, wants_spend_over_target)::decimal end as wants_gap_covered_saved  -- Excess cash used to cover annual Wants overspend after Needs, positive USD
        , case when excess_cash_after_needs_saved is null then null else greatest(wants_spend_over_target - excess_cash_after_needs_saved, 0)::decimal end as wants_gap_uncovered_spend  -- Annual Wants overspend still uncovered after excess cash, positive USD spend
        , case when excess_cash_after_needs_saved is null then null else greatest(excess_cash_after_needs_saved - wants_spend_over_target, 0)::decimal end as excess_cash_after_wants_saved  -- Excess cash remaining after annual Wants gap coverage, positive USD
    from needs_waterfall
)

, savings_waterfall as (
    select
        wants_waterfall.*
        , case when excess_cash_after_wants_saved is null then null else least(excess_cash_after_wants_saved, savings_saved_shortfall)::decimal end as savings_gap_covered_saved  -- Excess cash used to cover the annual Savings target shortfall after spending gaps, positive USD
        , case when excess_cash_after_wants_saved is null then null else greatest(savings_saved_shortfall - excess_cash_after_wants_saved, 0)::decimal end as savings_gap_uncovered_saved  -- Annual Savings target shortfall still uncovered after excess cash, positive USD
        , case when excess_cash_after_wants_saved is null then null else greatest(excess_cash_after_wants_saved - savings_saved_shortfall, 0)::decimal end as excess_cash_after_savings_saved  -- Excess cash remaining after annual Savings gap coverage, positive USD
    from wants_waterfall
)

, investments_waterfall as (
    select
        savings_waterfall.*
        , case when excess_cash_after_savings_saved is null then null else least(excess_cash_after_savings_saved, investments_saved_shortfall)::decimal end as investments_gap_covered_saved  -- Excess cash used to cover the annual Investments target shortfall after higher-priority gaps, positive USD
        , case when excess_cash_after_savings_saved is null then null else greatest(investments_saved_shortfall - excess_cash_after_savings_saved, 0)::decimal end as investments_gap_uncovered_saved  -- Annual Investments target shortfall still uncovered after excess cash, positive USD
        , case when excess_cash_after_savings_saved is null then null else greatest(excess_cash_after_savings_saved - investments_saved_shortfall, 0)::decimal end as remaining_excess_cash_saved  -- Excess cash left after all annual target gaps are filled, positive USD
    from savings_waterfall
)

select
    budget_year  -- Calendar budget year
    , net_income_to_account  -- Net income deposited to budget accounts, positive USD
    , plan_income  -- Income basis for the 50/30/15/5 targets from net income deposited to budget accounts, positive USD
    , greatest(coalesce(target_needs_spend, 0), 0)::decimal as target_needs_spend  -- 50% Needs spend target, non-negative USD
    , least(coalesce(actual_needs_spend, 0) * -1, 0)::decimal as actual_needs_spend  -- Actual Needs spend, non-positive USD
    , greatest(coalesce(target_needs_spend, 0), 0) + least(coalesce(actual_needs_spend, 0) * -1, 0) as needs_surplus_spend  -- Needs target plus signed spend; positive is remaining target and negative is over target
    , greatest(coalesce(target_wants_spend, 0), 0)::decimal as target_wants_spend  -- 30% Wants spend target, non-negative USD
    , least(coalesce(actual_wants_spend, 0) * -1, 0)::decimal as actual_wants_spend  -- Actual Wants spend, non-positive USD
    , greatest(coalesce(target_wants_spend, 0), 0) + least(coalesce(actual_wants_spend, 0) * -1, 0) as wants_surplus_spend  -- Wants target plus signed spend; positive is remaining target and negative is over target
    , greatest(coalesce(target_savings_saved, 0), 0)::decimal as target_savings_saved  -- 5% Savings target, non-negative USD
    , least(coalesce(actual_savings_saved, 0) * -1, 0)::decimal as actual_savings_saved  -- Actual Savings budgeted, non-positive USD
    , greatest(coalesce(target_savings_saved, 0), 0) + least(coalesce(actual_savings_saved, 0) * -1, 0) as savings_surplus_saved  -- Savings target plus signed saved amount; positive is remaining target and negative is over target
    , savings_balance_change_saved  -- Savings net after Savings and Emergency Fund spend, positive/negative USD
    , savings_rolling_balance_saved  -- Latest Savings and Emergency Fund balance in the year, USD
    , greatest(coalesce(target_investments_saved, 0), 0)::decimal as target_investments_saved  -- 15% Investments target, non-negative USD
    , least(coalesce(actual_investments_saved, 0) * -1, 0)::decimal as actual_investments_saved  -- Actual Investments saved, non-positive USD
    , greatest(coalesce(target_investments_saved, 0), 0) + least(coalesce(actual_investments_saved, 0) * -1, 0) as investments_surplus_saved  -- Investments target plus signed saved amount; positive is remaining target and negative is over target
    , overflow_dollars_saved  -- Annual income left after Needs, Wants, Savings saved, and Investments saved, positive/negative USD
    , buffer_target_saved  -- Latest average Needs and Wants spend target in the year, positive USD
    , buffer_balance_saved  -- Latest month-end Buffer Balance in the year, positive/negative USD
    , buffer_gap_saved  -- Latest additional Buffer Balance needed to cover the one-month Buffer Target, positive USD
    , buffer_surplus_saved  -- Latest Buffer Balance minus Buffer Target; positive is over target and negative is under target
    , overflow_to_buffer_saved  -- Annual overflow dollars needed to fill the latest one-month buffer gap, positive USD
    , used_from_buffer_saved  -- Latest negative monthly overflow covered by the starting buffer balance, positive USD
    , uncovered_shortfall_spend  -- Latest negative monthly overflow not covered by the starting buffer balance, positive USD spend
    , true_excess_saved  -- Latest Buffer Balance above the one-month Buffer Target, positive USD
    , emergency_fund_balance  -- Latest Emergency Fund category balance in the year, positive USD
    , emergency_fund_target_saved  -- Latest Emergency Fund target at three times the Buffer Target, positive USD
    , emergency_fund_gap_saved  -- Additional Emergency Fund balance needed to hit the three-month target, positive USD
    , emergency_fund_surplus_saved  -- Latest Emergency Fund balance minus the three-month target, positive/negative USD
    , reserve_surplus_saved  -- Combined cash reserve surplus after applying Emergency Fund surplus against Buffer shortfall, positive/negative USD
    , other_savings_balance_saved  -- Latest Savings balance outside the Emergency Fund bucket, USD
    , actual_rollover  -- Net-to-account income left after actual budget-account spend and budgeted saved amounts
    , runway_budget_month  -- Latest month represented in the yearly runway metrics
    , runway_cash_saved  -- Latest cash used for runway, positive USD
    , monthly_cash_spend  -- Average monthly Needs and Wants cash spend in the year, positive USD
    , avg_complete_monthly_cash_spend_3mo  -- Latest average Needs and Wants cash spend from the prior three complete months, positive USD
    , target_total_runway_cash_saved  -- Total runway target across rollover cash plus Emergency Fund, positive USD
    , required_rollover_cash_saved  -- Rollover cash needed to satisfy both one-month rollover and 3.5-month total-runway targets, positive USD
    , rollover_runway_months  -- Months of prior-average spend covered by latest runway cash
    , total_runway_months  -- Months of prior-average spend covered by latest runway cash plus Emergency Fund
    , rollover_cash_shortfall_saved  -- Additional runway cash needed before any surplus can fill annual category gaps, positive USD
    , excess_rollover_cash_saved  -- Latest runway cash above the protected runway target, positive USD
    , has_balanced_runway  -- True when latest runway cash satisfies both runway constraints
    , needs_gap_covered_saved  -- Excess cash used to cover annual Needs overspend before lower-priority gaps, positive USD
    , needs_gap_uncovered_spend  -- Annual Needs overspend still uncovered after excess cash, positive USD spend
    , excess_cash_after_needs_saved  -- Excess cash remaining after annual Needs gap coverage, positive USD
    , wants_gap_covered_saved  -- Excess cash used to cover annual Wants overspend after Needs, positive USD
    , wants_gap_uncovered_spend  -- Annual Wants overspend still uncovered after excess cash, positive USD spend
    , excess_cash_after_wants_saved  -- Excess cash remaining after annual Wants gap coverage, positive USD
    , savings_gap_covered_saved  -- Excess cash used to cover the annual Savings target shortfall after spending gaps, positive USD
    , savings_gap_uncovered_saved  -- Annual Savings target shortfall still uncovered after excess cash, positive USD
    , excess_cash_after_savings_saved  -- Excess cash remaining after annual Savings gap coverage, positive USD
    , investments_gap_covered_saved  -- Excess cash used to cover the annual Investments target shortfall after higher-priority gaps, positive USD
    , investments_gap_uncovered_saved  -- Annual Investments target shortfall still uncovered after excess cash, positive USD
    , remaining_excess_cash_saved  -- Excess cash left after all annual target gaps are filled, positive USD
    , investments_gap_covered_saved + remaining_excess_cash_saved as total_cash_available_to_invest_saved  -- Cash available for retroactive investment budgeting after higher-priority gaps and runway targets, positive USD
from investments_waterfall
order by budget_year desc
