MODEL (
  name core.yearly_bucket_adherence,
  kind FULL,
  grain (year, bucket),
  audits (
    not_null(columns := (year, bucket)),
    unique_combination_of_columns(columns := (year, bucket)),
    yearly_bucket_adherence_overage_consistency,
    yearly_bucket_adherence_on_plan_consistency
  ),
  description 'Per-year, per-bucket (Needs/Wants) 50/30/20 adherence: target from income derivation, projected spend rolled up from category adherence, overage %, and on-plan flag.'
);

with bucket_projected as (
    /*
        Roll category-level projections up to (year, bucket). projected is the
        sum of per-category projected_current_year (current-year extrapolation
        or actual for past years).
    */
    select
        year
        , bucket
        , round(sum(projected_current_year), 2) as projected
        , bool_or(is_extrapolated) as is_extrapolated
    from core.yearly_category_adherence
    group by 1, 2
)

, bucket_targets as (
    /*
        Unpivot the §1 50/30/20 targets into one row per (year, bucket) so the
        rollup joins on a single key. The Investments target is intentionally
        excluded — Investments adherence is tracked by
        core.yearly_investment_contributions, not here.
    */
    select year, 'Needs' as bucket, needs_target as target
    from core.yearly_income_derivation
    union all
    select year, 'Wants' as bucket, wants_target as target
    from core.yearly_income_derivation
)

select
    bucket_projected.year as year
    , bucket_projected.bucket as bucket
    , bucket_targets.target as target
    , bucket_projected.projected as projected
    , case
        when bucket_targets.target is null or bucket_targets.target = 0
            then null
        else round(
            (bucket_projected.projected - bucket_targets.target)
            / bucket_targets.target
            , 4
        )
    end as overage_pct
    , case
        when bucket_targets.target is null then null
        else bucket_projected.projected <= bucket_targets.target
    end as on_plan_flag
    , bucket_projected.is_extrapolated as is_extrapolated
from bucket_projected
left join bucket_targets
    on bucket_projected.year = bucket_targets.year
    and bucket_projected.bucket = bucket_targets.bucket
order by bucket_projected.year desc, bucket_projected.bucket
