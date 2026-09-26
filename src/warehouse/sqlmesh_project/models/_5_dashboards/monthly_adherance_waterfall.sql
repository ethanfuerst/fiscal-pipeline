MODEL (
  name dashboards.monthly_adherance_waterfall,
  kind FULL,
  grain budget_month,
  description 'Monthly 50/30/15/5 waterfall showing category targets, overflow dollars, buffer state, and emergency fund state.'
);

with monthly_base as (
    select
        budget_month
        , net_income_to_account
        , plan_income
        , emergency_fund_balance
        , target_needs_spend
        , actual_needs_spend
        , needs_spend_over_target
        , needs_spend_under_target
        , target_wants_spend
        , actual_wants_spend
        , wants_spend_over_target
        , wants_spend_under_target
        , target_investments_saved
        , actual_investments_saved
        , investments_saved_shortfall
        , investments_saved_over_target
        , target_savings_saved
        , actual_savings_saved
        , savings_balance_change_saved
        , savings_rolling_balance_saved
        , savings_saved_shortfall
        , savings_saved_over_target
        , actual_rollover
        , monthly_cash_spend
    from core.monthly_adherance
)

/*
    Buffer Balance uses two assignment concepts:
    - total_assigned supports existing runway-style metrics and intentionally remains the raw YNAB assigned total.
    - credit_card_assigned is excluded only from the Buffer Balance running ledger. Credit Card Payments assignments can represent
      paydown of credit overspending that did not originate as Income-category inflow, so including them understates cash banked
      from income.

    needs_wants_balance is the YNAB available balance in the live buffer categories at month end. It is combined later with the
    income-funded, not-yet-assigned ledger balance so the dashboard captures both money sitting in Needs/Wants categories and
    income already received that is effectively pre-funding future budget months.
*/
, monthly_assignments as (
    select
        budget_month
        , coalesce(sum(assigned), 0)::decimal as total_assigned  -- Net dollars assigned in the budget month, positive/negative USD
        , coalesce(sum(if(category_group_name_mapping = 'Credit Card Payments', assigned, 0)), 0)::decimal as credit_card_assigned  -- Credit card debt paydown assignments, funded by credit overspending rather than income, positive/negative USD
        , coalesce(sum(if(category_group_name_mapping in ('Needs', 'Wants'), balance, 0)), 0)::decimal as needs_wants_balance  -- Needs and Wants available balance at month end, positive/negative USD
    from combined.monthly_budgeted
    group by budget_month
)

, trailing_complete_months as (
    select
        monthly_base.budget_month
        , monthly_base.net_income_to_account
        , monthly_base.plan_income
        , monthly_base.emergency_fund_balance
        , monthly_base.target_needs_spend
        , monthly_base.actual_needs_spend
        , monthly_base.needs_spend_over_target
        , monthly_base.needs_spend_under_target
        , monthly_base.target_wants_spend
        , monthly_base.actual_wants_spend
        , monthly_base.wants_spend_over_target
        , monthly_base.wants_spend_under_target
        , monthly_base.target_investments_saved
        , monthly_base.actual_investments_saved
        , monthly_base.investments_saved_shortfall
        , monthly_base.investments_saved_over_target
        , monthly_base.target_savings_saved
        , monthly_base.actual_savings_saved
        , monthly_base.savings_balance_change_saved
        , monthly_base.savings_rolling_balance_saved
        , monthly_base.savings_saved_shortfall
        , monthly_base.savings_saved_over_target
        , monthly_base.actual_rollover
        , monthly_base.monthly_cash_spend
        , coalesce(monthly_assignments.total_assigned, 0)::decimal as total_assigned
        , coalesce(monthly_assignments.credit_card_assigned, 0)::decimal as credit_card_assigned
        , coalesce(monthly_assignments.needs_wants_balance, 0)::decimal as needs_wants_balance
        , greatest(coalesce(monthly_base.net_income_to_account, 0) - coalesce(monthly_assignments.total_assigned, 0), 0)::decimal as runway_cash_saved
        , count(monthly_base.monthly_cash_spend) over (order by monthly_base.budget_month rows between 3 preceding and 1 preceding) as complete_months_in_runway_average
        , avg(monthly_base.monthly_cash_spend) over (order by monthly_base.budget_month rows between 3 preceding and 1 preceding) as prior_avg_monthly_cash_spend_3mo
        , count(monthly_base.monthly_cash_spend) over (
            partition by extract('year' from monthly_base.budget_month)
            order by monthly_base.budget_month
            rows between unbounded preceding and 1 preceding
        ) as complete_months_in_year_buffer_average
        , avg(monthly_base.monthly_cash_spend) over (
            partition by extract('year' from monthly_base.budget_month)
            order by monthly_base.budget_month
            rows between unbounded preceding and 1 preceding
        ) as year_to_date_buffer_average
        , avg(monthly_base.monthly_cash_spend) over (
            order by monthly_base.budget_month
            rows between 6 preceding and 1 preceding
        ) as trailing_6mo_buffer_average
    from monthly_base
    left join monthly_assignments
        on monthly_base.budget_month = monthly_assignments.budget_month
)

, runway_targets as (
    select
        budget_month
        , net_income_to_account
        , plan_income
        , emergency_fund_balance
        , target_needs_spend
        , actual_needs_spend
        , needs_spend_over_target
        , needs_spend_under_target
        , target_wants_spend
        , actual_wants_spend
        , wants_spend_over_target
        , wants_spend_under_target
        , target_investments_saved
        , actual_investments_saved
        , investments_saved_shortfall
        , investments_saved_over_target
        , target_savings_saved
        , actual_savings_saved
        , savings_balance_change_saved
        , savings_rolling_balance_saved
        , savings_saved_shortfall
        , savings_saved_over_target
        , actual_rollover
        , monthly_cash_spend
        , total_assigned
        , credit_card_assigned
        , needs_wants_balance
        , runway_cash_saved
        , complete_months_in_runway_average
        , case
            when complete_months_in_year_buffer_average >= 6 then year_to_date_buffer_average
            else trailing_6mo_buffer_average
        end as buffer_target_saved  -- Average Needs and Wants spend from current-year complete months, or trailing six complete months when fewer than six current-year months exist
        , case when complete_months_in_runway_average = 3 then prior_avg_monthly_cash_spend_3mo end as avg_complete_monthly_cash_spend_3mo  -- Average Needs and Wants cash spend from the prior three complete months, positive USD
        , case when complete_months_in_runway_average = 3 then prior_avg_monthly_cash_spend_3mo * 3.50 end as target_total_runway_cash_saved  -- Total runway target across rollover cash plus Emergency Fund, positive USD
        , case
            when complete_months_in_runway_average = 3
                then greatest(prior_avg_monthly_cash_spend_3mo, prior_avg_monthly_cash_spend_3mo * 3.50 - emergency_fund_balance, 0)::decimal
        end as required_rollover_cash_saved  -- Rollover cash needed to satisfy both one-month rollover and 3.5-month total-runway targets, positive USD
    from trailing_complete_months
)

, runway_state as (
    select
        budget_month
        , net_income_to_account
        , plan_income
        , emergency_fund_balance
        , target_needs_spend
        , actual_needs_spend
        , needs_spend_over_target
        , needs_spend_under_target
        , target_wants_spend
        , actual_wants_spend
        , wants_spend_over_target
        , wants_spend_under_target
        , target_investments_saved
        , actual_investments_saved
        , investments_saved_shortfall
        , investments_saved_over_target
        , target_savings_saved
        , actual_savings_saved
        , savings_balance_change_saved
        , savings_rolling_balance_saved
        , savings_saved_shortfall
        , savings_saved_over_target
        , actual_rollover
        , monthly_cash_spend
        , total_assigned
        , credit_card_assigned
        , needs_wants_balance
        , buffer_target_saved
        , runway_cash_saved
        , avg_complete_monthly_cash_spend_3mo
        , target_total_runway_cash_saved
        , required_rollover_cash_saved
        , case when avg_complete_monthly_cash_spend_3mo = 0 then null else round(runway_cash_saved / avg_complete_monthly_cash_spend_3mo, 4) end as rollover_runway_months  -- Months of prior-average spend covered by runway cash
        , case when avg_complete_monthly_cash_spend_3mo = 0 then null else round((runway_cash_saved + emergency_fund_balance) / avg_complete_monthly_cash_spend_3mo, 4) end as total_runway_months  -- Months of prior-average spend covered by runway cash plus Emergency Fund
        , case when required_rollover_cash_saved is null then null else greatest(required_rollover_cash_saved - runway_cash_saved, 0)::decimal end as rollover_cash_shortfall_saved  -- Additional runway cash needed before any surplus can fill category gaps, positive USD
        , case when required_rollover_cash_saved is null then null else greatest(runway_cash_saved - required_rollover_cash_saved, 0)::decimal end as excess_rollover_cash_saved  -- Runway cash above the protected runway target, positive USD
        , required_rollover_cash_saved is not null and runway_cash_saved >= required_rollover_cash_saved as has_balanced_runway  -- True when runway cash satisfies both runway constraints
    from runway_targets
)

, overflow_months as (
    select
        runway_state.*
        , (
            coalesce(net_income_to_account, 0)
            - coalesce(actual_needs_spend, 0)
            - coalesce(actual_wants_spend, 0)
            - coalesce(actual_savings_saved, 0)
            - coalesce(actual_investments_saved, 0)
        )::decimal as overflow_dollars_saved  -- Income left after current Needs and Wants spend plus Savings and Investments saved, positive/negative USD
    from runway_state
)

/*
    Buffer Balance is a through-month accounting identity, not a look-ahead into future budget months:

        Needs/Wants available at month end
        + cumulative Income-category inflow through month end
        - cumulative assigned dollars through month end, excluding Credit Card Payments

    The cumulative income-minus-non-credit-card-assigned term represents income received by that month that has not been consumed
    by assignments through that month. In the current month, that same term can reconcile to money already assigned in future
    budget months, but the formula itself never reads future-month assignments or balances. This keeps historical rows defensible:
    each month's Buffer Balance uses only information available at that month end.

    This also makes the metric invariant to whether Needs/Wants balances are swept out of old months. Sweeping lowers the
    Needs/Wants balance term and raises the banked-income term by the same amount.
*/
, buffer_running as (
    select
        overflow_months.*
        , (
            coalesce(lag(needs_wants_balance) over (order by budget_month), 0)
            + coalesce(
                sum(net_income_to_account) over (order by budget_month rows between unbounded preceding and 1 preceding),
                0
            )
            - coalesce(
                sum(total_assigned - credit_card_assigned) over (order by budget_month rows between unbounded preceding and 1 preceding),
                0
            )
        )::decimal as starting_buffer_balance_saved  -- Buffer Balance at the end of the prior month, positive/negative USD
        , sum(overflow_dollars_saved) over (order by budget_month rows between unbounded preceding and current row)::decimal as running_overflow_balance_saved  -- Running total of monthly overflow dollars, positive/negative USD
        , (
            needs_wants_balance
            + sum(net_income_to_account) over (order by budget_month rows between unbounded preceding and current row)
            - sum(total_assigned - credit_card_assigned) over (order by budget_month rows between unbounded preceding and current row)
        )::decimal as buffer_balance_saved  -- Month-end Needs and Wants available plus income received but not yet assigned to any month through month end; credit card debt assignments excluded because credit overspending funds them, not income
    from overflow_months
)

, overflow_state as (
    select
        buffer_running.*
        , case
            when buffer_target_saved is null then null
            else greatest(buffer_target_saved - buffer_balance_saved, 0)::decimal
        end as buffer_gap_saved  -- Additional Buffer Balance needed to cover the one-month Buffer Target, positive USD
        , case
            when buffer_target_saved is null then null
            else buffer_balance_saved - buffer_target_saved
        end as buffer_surplus_saved  -- Buffer Balance minus Buffer Target; positive is over target and negative is under target
        , case
            when buffer_target_saved is null then null
            else least(
                greatest(overflow_dollars_saved, 0),
                greatest(buffer_target_saved - buffer_balance_saved, 0)
            )::decimal
        end as overflow_to_buffer_saved  -- Current-month overflow dollars needed to fill the buffer gap, positive USD
        , least(greatest(overflow_dollars_saved * -1, 0), greatest(starting_buffer_balance_saved, 0))::decimal as used_from_buffer_saved  -- Negative current-month overflow covered by the starting Buffer Balance, positive USD
        , greatest(greatest(overflow_dollars_saved * -1, 0) - greatest(starting_buffer_balance_saved, 0), 0)::decimal as uncovered_shortfall_spend  -- Negative current-month overflow not covered by the starting Buffer Balance, positive USD spend
        , case
            when buffer_target_saved is null then null
            else greatest(buffer_balance_saved - buffer_target_saved, 0)::decimal
        end as true_excess_saved  -- Buffer Balance above the one-month Buffer Target, positive USD
        , case when buffer_target_saved is null then null else buffer_target_saved * 3 end as emergency_fund_target_saved  -- Emergency Fund target at three times the Buffer Target, positive USD
        , case
            when buffer_target_saved is null then null
            else greatest(buffer_target_saved * 3 - coalesce(emergency_fund_balance, 0), 0)::decimal
        end as emergency_fund_gap_saved  -- Additional Emergency Fund balance needed to hit the three-month target, positive USD
        , case
            when buffer_target_saved is null then null
            else coalesce(emergency_fund_balance, 0) - buffer_target_saved * 3
        end as emergency_fund_surplus_saved  -- Emergency Fund balance minus the three-month target, positive/negative USD
        , case
            when buffer_target_saved is null then null
            else coalesce(emergency_fund_balance, 0) - buffer_target_saved * 3 + (buffer_balance_saved - buffer_target_saved)
        end as reserve_surplus_saved  -- Combined cash reserve surplus after applying Emergency Fund surplus against Buffer shortfall, positive/negative USD
        , (coalesce(savings_rolling_balance_saved, 0) - coalesce(emergency_fund_balance, 0))::decimal as other_savings_balance_saved  -- Savings balance outside the Emergency Fund bucket, USD
    from buffer_running
)

, needs_waterfall as (
    select
        overflow_state.*
        , case when excess_rollover_cash_saved is null then null else least(excess_rollover_cash_saved, needs_spend_over_target)::decimal end as needs_gap_covered_saved  -- Excess cash used to cover Needs overspend before lower-priority gaps, positive USD
        , case when excess_rollover_cash_saved is null then null else greatest(needs_spend_over_target - excess_rollover_cash_saved, 0)::decimal end as needs_gap_uncovered_spend  -- Needs overspend still uncovered after excess cash, positive USD spend
        , case when excess_rollover_cash_saved is null then null else greatest(excess_rollover_cash_saved - needs_spend_over_target, 0)::decimal end as excess_cash_after_needs_saved  -- Excess cash remaining after Needs gap coverage, positive USD
    from overflow_state
)

, wants_waterfall as (
    select
        needs_waterfall.*
        , case when excess_cash_after_needs_saved is null then null else least(excess_cash_after_needs_saved, wants_spend_over_target)::decimal end as wants_gap_covered_saved  -- Excess cash used to cover Wants overspend after Needs, positive USD
        , case when excess_cash_after_needs_saved is null then null else greatest(wants_spend_over_target - excess_cash_after_needs_saved, 0)::decimal end as wants_gap_uncovered_spend  -- Wants overspend still uncovered after excess cash, positive USD spend
        , case when excess_cash_after_needs_saved is null then null else greatest(excess_cash_after_needs_saved - wants_spend_over_target, 0)::decimal end as excess_cash_after_wants_saved  -- Excess cash remaining after Wants gap coverage, positive USD
    from needs_waterfall
)

, savings_waterfall as (
    select
        wants_waterfall.*
        , case when excess_cash_after_wants_saved is null then null else least(excess_cash_after_wants_saved, savings_saved_shortfall)::decimal end as savings_gap_covered_saved  -- Excess cash used to cover the Savings target shortfall after spending gaps, positive USD
        , case when excess_cash_after_wants_saved is null then null else greatest(savings_saved_shortfall - excess_cash_after_wants_saved, 0)::decimal end as savings_gap_uncovered_saved  -- Savings target shortfall still uncovered after excess cash, positive USD
        , case when excess_cash_after_wants_saved is null then null else greatest(excess_cash_after_wants_saved - savings_saved_shortfall, 0)::decimal end as excess_cash_after_savings_saved  -- Excess cash remaining after Savings gap coverage, positive USD
    from wants_waterfall
)

, investments_waterfall as (
    select
        savings_waterfall.*
        , case when excess_cash_after_savings_saved is null then null else least(excess_cash_after_savings_saved, investments_saved_shortfall)::decimal end as investments_gap_covered_saved  -- Excess cash used to cover the Investments target shortfall after higher-priority gaps, positive USD
        , case when excess_cash_after_savings_saved is null then null else greatest(investments_saved_shortfall - excess_cash_after_savings_saved, 0)::decimal end as investments_gap_uncovered_saved  -- Investments target shortfall still uncovered after excess cash, positive USD
        , case when excess_cash_after_savings_saved is null then null else greatest(excess_cash_after_savings_saved - investments_saved_shortfall, 0)::decimal end as remaining_excess_cash_saved  -- Excess cash left after all target gaps are filled, positive USD
    from savings_waterfall
)

select
    budget_month  -- First day of the budget month
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
    , savings_rolling_balance_saved  -- Savings and Emergency Fund balance at month end, USD
    , greatest(coalesce(target_investments_saved, 0), 0)::decimal as target_investments_saved  -- 15% Investments target, non-negative USD
    , least(coalesce(actual_investments_saved, 0) * -1, 0)::decimal as actual_investments_saved  -- Actual Investments saved, non-positive USD
    , greatest(coalesce(target_investments_saved, 0), 0) + least(coalesce(actual_investments_saved, 0) * -1, 0) as investments_surplus_saved  -- Investments target plus signed saved amount; positive is remaining target and negative is over target
    , overflow_dollars_saved  -- Income left after Needs, Wants, Savings saved, and Investments saved, positive/negative USD
    , buffer_target_saved  -- Average Needs and Wants spend from the current year or trailing six complete months, positive USD
    , buffer_balance_saved  -- Month-end Needs and Wants available plus income banked for future months, positive/negative USD
    , buffer_gap_saved  -- Additional Buffer Balance needed to cover the one-month Buffer Target, positive USD
    , buffer_surplus_saved  -- Buffer Balance minus Buffer Target; positive is over target and negative is under target
    , overflow_to_buffer_saved  -- Current-month overflow dollars needed to fill the one-month buffer gap, positive USD
    , used_from_buffer_saved  -- Negative current-month overflow covered by the starting buffer balance, positive USD
    , uncovered_shortfall_spend  -- Negative current-month overflow not covered by the starting buffer balance, positive USD spend
    , true_excess_saved  -- Buffer Balance above the one-month Buffer Target, positive USD
    , emergency_fund_balance  -- Emergency Fund category balance at month end, positive USD
    , emergency_fund_target_saved  -- Emergency Fund target at three times the Buffer Target, positive USD
    , emergency_fund_gap_saved  -- Additional Emergency Fund balance needed to hit the three-month target, positive USD
    , emergency_fund_surplus_saved  -- Emergency Fund balance minus the three-month target, positive/negative USD
    , reserve_surplus_saved  -- Combined cash reserve surplus after applying Emergency Fund surplus against Buffer shortfall, positive/negative USD
    , other_savings_balance_saved  -- Savings balance outside the Emergency Fund bucket, USD
    , actual_rollover  -- Net-to-account income left after actual budget-account spend and budgeted saved amounts
    , runway_cash_saved  -- Net income not assigned in the budget month, assumed to be budgeted into future months, positive USD
    , monthly_cash_spend  -- Actual Needs and Wants cash spend used for runway, positive USD
    , avg_complete_monthly_cash_spend_3mo  -- Average Needs and Wants cash spend from the prior three complete months, positive USD
    , target_total_runway_cash_saved  -- Total runway target across rollover cash plus Emergency Fund, positive USD
    , required_rollover_cash_saved  -- Rollover cash needed to satisfy both one-month rollover and 3.5-month total-runway targets, positive USD
    , rollover_runway_months  -- Months of prior-average spend covered by runway cash
    , total_runway_months  -- Months of prior-average spend covered by runway cash plus Emergency Fund
    , rollover_cash_shortfall_saved  -- Additional runway cash needed before any surplus can fill category gaps, positive USD
    , excess_rollover_cash_saved  -- Runway cash above the protected runway target, positive USD
    , has_balanced_runway  -- True when runway cash satisfies both runway constraints
    , needs_gap_covered_saved  -- Excess cash used to cover Needs overspend before lower-priority gaps, positive USD
    , needs_gap_uncovered_spend  -- Needs overspend still uncovered after excess cash, positive USD spend
    , excess_cash_after_needs_saved  -- Excess cash remaining after Needs gap coverage, positive USD
    , wants_gap_covered_saved  -- Excess cash used to cover Wants overspend after Needs, positive USD
    , wants_gap_uncovered_spend  -- Wants overspend still uncovered after excess cash, positive USD spend
    , excess_cash_after_wants_saved  -- Excess cash remaining after Wants gap coverage, positive USD
    , savings_gap_covered_saved  -- Excess cash used to cover the Savings target shortfall after spending gaps, positive USD
    , savings_gap_uncovered_saved  -- Savings target shortfall still uncovered after excess cash, positive USD
    , excess_cash_after_savings_saved  -- Excess cash remaining after Savings gap coverage, positive USD
    , investments_gap_covered_saved  -- Excess cash used to cover the Investments target shortfall after higher-priority gaps, positive USD
    , investments_gap_uncovered_saved  -- Investments target shortfall still uncovered after excess cash, positive USD
    , remaining_excess_cash_saved  -- Excess cash left after all target gaps are filled, positive USD
    , investments_gap_covered_saved + remaining_excess_cash_saved as total_cash_available_to_invest_saved  -- Cash available for retroactive investment budgeting after higher-priority gaps and runway targets, positive USD
from investments_waterfall
order by budget_month desc
