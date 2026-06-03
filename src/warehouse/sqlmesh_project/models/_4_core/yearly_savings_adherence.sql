MODEL (
  name core.yearly_savings_adherence,
  kind FULL,
  grain (year),
  audits (
    not_null(columns := (year, bucket)),
    unique_values(columns := (year)),
    yearly_savings_adherence_overage_consistency,
    yearly_savings_adherence_on_plan_consistency
  ),
  description 'Per-year Savings-bucket (5%) adherence. Savings is the single YNAB `Savings` category. Net saved = budgeted - spent, where spend withdrawn from savings is meant to be covered by §8 extra income: net_saved = savings_budgeted - max(0, savings_spent - extra_covered). Unlike Needs/Wants, more is better: on_plan_flag is projected >= target. This model emits the PRE-COVERAGE view (extra_covered = 0) plus the savings_budgeted / savings_spent components the §8 extra-income patch consumes; the coverage-adjusted adherence is produced downstream once §8 (and its §1 extra-income input) exist. Investments and Savings are the two more-is-good buckets, both excluded from core.yearly_bucket_adherence.'
);

with savings_months as (
    /*
        Per-month budgeted and net activity for the single `Savings` category.
        The `Savings` group holds exactly this one category, so filtering the
        group isolates it. activity_usd is negative for spend in YNAB.

        Exclude budget months that have not elapsed yet: YNAB pre-creates
        future budget-month rows (zero activity) for scheduled/goal categories,
        which would inflate months_with_data and distort the current-year
        extrapolation.

        Join monthly_categories.category_group_id directly to
        category_groups.id rather than hopping through cleaned.categories — the
        as-of-month affiliation lives on monthly_categories itself.
    */
    select
        monthly_categories.year as year
        , monthly_categories.month as month
        , sum(monthly_categories.budgeted_usd) as budgeted_usd
        , sum(monthly_categories.activity_usd) as activity_usd
    from cleaned.monthly_categories as monthly_categories
    inner join cleaned.category_groups as category_groups
        on monthly_categories.category_group_id = category_groups.id
    where category_groups.category_group_name_mapping = 'Savings'
        and coalesce(monthly_categories.deleted, false) = false
        and make_date(monthly_categories.year, monthly_categories.month, 1)
            <= date_trunc('month', current_date())
    group by 1, 2
)

, savings_years as (
    /*
        Year-level YTD assigned and withdrawn dollars, plus the count of
        elapsed budget months that drives current-year extrapolation.
        savings_spent_ytd is net withdrawal (sign-flipped activity) floored at
        0 so a net-inflow year reads as zero spend, not negative.
    */
    select
        year
        , round(sum(budgeted_usd), 2) as savings_budgeted_ytd
        , round(greatest(0, -1 * sum(activity_usd)), 2) as savings_spent_ytd
        , count(distinct month) as months_with_data
    from savings_months
    group by 1
)

, projected as (
    /*
        Drive off income_derivation so every year with a savings_target gets a
        row even when no savings activity exists yet. Current year extrapolates
        YTD assigned and withdrawn across elapsed months to a full year; past
        years pass YTD through.

        extra_covered is the §8 extra-income coverage of savings spend. It is
        hard-zero here: the §8 patch model (and its §1 extra-income input) do
        not exist yet, so this model reports the pre-coverage floor. When §8
        lands, the coverage-adjusted net_saved = savings_budgeted -
        max(0, savings_spent - extra_covered) is produced downstream, consuming
        the savings_spent component surfaced below.
    */
    select
        income.year as year
        , income.savings_target as target
        , income.year = cast(extract('year' from current_date()) as integer) as is_extrapolated
        , case
            when income.year = cast(extract('year' from current_date()) as integer)
                 and coalesce(savings_years.months_with_data, 0) > 0
                then round(savings_years.savings_budgeted_ytd / savings_years.months_with_data * 12, 2)
            else round(coalesce(savings_years.savings_budgeted_ytd, 0), 2)
        end as savings_budgeted
        , case
            when income.year = cast(extract('year' from current_date()) as integer)
                 and coalesce(savings_years.months_with_data, 0) > 0
                then round(savings_years.savings_spent_ytd / savings_years.months_with_data * 12, 2)
            else round(coalesce(savings_years.savings_spent_ytd, 0), 2)
        end as savings_spent
        , 0::decimal as extra_covered  -- §8 coverage not yet wired; pre-coverage floor
    from core.yearly_income_derivation as income
    left join savings_years
        on income.year = savings_years.year
)

, netted as (
    select
        year
        , target
        , savings_budgeted
        , savings_spent
        , extra_covered
        , round(savings_budgeted - greatest(0, savings_spent - extra_covered), 2) as projected
        , is_extrapolated
    from projected
)

select
    year
    , 'Savings' as bucket
    , target
    , savings_budgeted
    , savings_spent
    , extra_covered
    , projected  -- Pre-coverage net saved (budgeted - spent); §8 coverage credits covered spend back downstream
    , case
        when target is null or target = 0
            then null
        else round((projected - target) / target, 4)
    end as overage_pct  -- More-is-good: positive means the savings goal was exceeded
    , case
        when target is null then null
        else projected >= target
    end as on_plan_flag  -- More-is-good: on plan when projected meets or beats target
    , is_extrapolated
from netted
order by year desc
