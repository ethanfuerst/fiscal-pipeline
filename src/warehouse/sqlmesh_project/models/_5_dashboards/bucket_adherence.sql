MODEL (
  name dashboards.bucket_adherence,
  kind FULL,
  grain (year, bucket),
  audits (
    not_null(columns := (year, bucket)),
    unique_combination_of_columns(columns := (year, bucket))
  ),
  description 'Tab 2 (ETH-472): 50/30/15/5 bucket adherence, one row per (year, bucket). Unions Needs/Wants (core.yearly_bucket_adherence) + Savings (core.yearly_savings_adherence) + Investments (core.yearly_investment_contributions, normalized: target = with-surplus, projected = investments_actual_usd, on_plan = investments_on_plan_after_surplus_flag). on_plan_flag already encodes polarity (Needs/Wants less-is-good, Investments/Savings more-is-good).'
);

with needs_wants as (
    select year, bucket, target, projected, overage_pct, on_plan_flag, is_extrapolated
    from core.yearly_bucket_adherence
)

, savings as (
    select year, bucket, target, projected, overage_pct, on_plan_flag, is_extrapolated
    from core.yearly_savings_adherence
)

, investments as (
    select
        year
        , 'Investments' as bucket
        , investments_target_with_surplus_usd as target
        , investments_actual_usd as projected
        , case
            when investments_target_with_surplus_usd = 0 then null
            else round(
                (investments_actual_usd - investments_target_with_surplus_usd)
                / investments_target_with_surplus_usd, 4)
          end as overage_pct
        , investments_on_plan_after_surplus_flag as on_plan_flag
        , is_current_year as is_extrapolated
    from core.yearly_investment_contributions
)

, unioned as (
    select * from needs_wants
    union all select * from savings
    union all select * from investments
)

select
    year
    , bucket
    , target
    , projected
    , overage_pct
    , on_plan_flag
    , is_extrapolated
from unioned
order by
    year desc
    , case bucket when 'Needs' then 1 when 'Wants' then 2
                  when 'Investments' then 3 when 'Savings' then 4 else 5 end
