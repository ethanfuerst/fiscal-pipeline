MODEL (
  name core.yearly_extra_income_allocation,
  kind FULL,
  grain (year),
  audits (
    not_null(columns := (year)),
    unique_values(columns := (year)),
    yearly_extra_income_allocation_conservation,
    yearly_extra_income_allocation_savings_on_plan_consistency,
    yearly_extra_income_allocation_overage_on_plan_consistency
  ),
  description 'Per-year §8 extra-income patch: waterfalls extra_income (§1 net one-off inflows) across Needs overage → Wants overage → Savings coverage → Investments surplus. Emits the four patch figures (which sum to extra_income) plus the coverage-adjusted savings net_saved / on_plan and the per-bucket on-plan-after-allocation flags (needs/wants on plan when the patch fully absorbed the overage; savings on plan when net_saved_after_coverage >= target). The Investments on-plan-after-surplus flag lives in core.yearly_investment_contributions (importing investment actuals here would form a cycle). Overage patches do NOT inflate Needs/Wants targets (buckets stay flagged in core.yearly_bucket_adherence); savings coverage credits covered spend back without raising the savings target or funding new savings. extra_income_surplus_to_investments feeds core.yearly_investment_contributions (ETH-470 step 4), where it augments the salary-based investments_target and re-splits 50/50 with employee-limit spillover.'
);

with bucket_pivot as (
    /*
        Pivot core.yearly_bucket_adherence from one row per (year, bucket) to
        one row per year with Needs/Wants projected + target side by side, so the
        waterfall joins on a single year key. is_extrapolated rolls up with
        bool_or (current-year extrapolation present in either bucket).
    */
    select
        year
        , max(case when bucket = 'Needs' then projected end) as needs_projected
        , max(case when bucket = 'Needs' then target end) as needs_target
        , max(case when bucket = 'Wants' then projected end) as wants_projected
        , max(case when bucket = 'Wants' then target end) as wants_target
        , bool_or(is_extrapolated) as is_extrapolated
    from core.yearly_bucket_adherence
    group by 1
)

, inputs as (
    /*
        Drive off income_derivation so every year with an extra_income figure
        gets a row even when no overage / savings spend exists. extra_income is
        actual YTD (never extrapolated) per §1; bucket projections and
        savings_spent ARE extrapolated for the current year.
    */
    select
        income.year as year
        , coalesce(income.extra_income, 0) as extra_income
        , coalesce(bucket_pivot.needs_projected, 0) as needs_projected
        , coalesce(bucket_pivot.needs_target, 0) as needs_target
        , coalesce(bucket_pivot.wants_projected, 0) as wants_projected
        , coalesce(bucket_pivot.wants_target, 0) as wants_target
        , coalesce(savings.savings_budgeted, 0) as savings_budgeted
        , coalesce(savings.savings_spent, 0) as savings_spent
        , savings.target as savings_target
        , coalesce(
            bucket_pivot.is_extrapolated, savings.is_extrapolated, income.is_extrapolated
          ) as is_extrapolated
    from core.yearly_income_derivation as income
    left join bucket_pivot
        on income.year = bucket_pivot.year
    left join core.yearly_savings_adherence as savings
        on income.year = savings.year
)

, needs_step as (
    /* Priority 1: patch Needs overage. Bucket stays flagged upstream. */
    select
        inputs.*
        , greatest(0, needs_projected - needs_target) as needs_overage
        , round(
            least(extra_income, greatest(0, needs_projected - needs_target))
            , 2
          ) as extra_income_used_for_needs_overage
    from inputs
)

, wants_step as (
    /* Priority 2: patch Wants overage with whatever remains after Needs. */
    select
        needs_step.*
        , round(extra_income - extra_income_used_for_needs_overage, 2)
            as remaining_after_needs
        , round(
            least(
                extra_income - extra_income_used_for_needs_overage,
                greatest(0, wants_projected - wants_target)
            )
            , 2
          ) as extra_income_used_for_wants_overage
    from needs_step
)

, savings_step as (
    /*
        Priority 3: cover Savings withdrawals (spend only) with what remains.
        Caps at savings_spent — coverage backfills withdrawn dollars, it never
        funds new savings assignments or raises the savings target.
    */
    select
        wants_step.*
        , round(remaining_after_needs - extra_income_used_for_wants_overage, 2)
            as remaining_after_wants
        , round(
            least(
                remaining_after_needs - extra_income_used_for_wants_overage,
                savings_spent
            )
            , 2
          ) as extra_income_used_for_savings_coverage
    from wants_step
)

select
    year
    , extra_income
    , extra_income_used_for_needs_overage
    , extra_income_used_for_wants_overage
    , extra_income_used_for_savings_coverage
    /* Priority 4: surplus absorbs the remainder; the four outputs sum to extra_income. */
    , round(remaining_after_wants - extra_income_used_for_savings_coverage, 2)
        as extra_income_surplus_to_investments
    /* Coverage-adjusted savings net saved (computed from the pre-coverage savings model's components). */
    , round(
        savings_budgeted - greatest(0, savings_spent - extra_income_used_for_savings_coverage)
        , 2
      ) as net_saved_after_coverage
    , case
        when savings_target is null then null
        else (savings_budgeted - greatest(0, savings_spent - extra_income_used_for_savings_coverage))
             >= savings_target
      end as savings_on_plan_after_coverage
    /*
        Per-bucket on-plan-after-allocation flags (one per budget bucket).
        Needs/Wants are less-is-good: on plan when the extra-income patch fully
        absorbed the overage (post-patch effective spend <= target). Null when the
        bucket has no target (no data). The Investments flag lives in
        core.yearly_investment_contributions (it needs investment actuals, which
        would form a circular dependency if imported here).
    */
    , case
        when needs_target = 0 then null
        else (needs_projected - extra_income_used_for_needs_overage) <= needs_target
      end as needs_on_plan_after_allocation
    , case
        when wants_target = 0 then null
        else (wants_projected - extra_income_used_for_wants_overage) <= wants_target
      end as wants_on_plan_after_allocation
    , is_extrapolated
from savings_step
order by year desc
