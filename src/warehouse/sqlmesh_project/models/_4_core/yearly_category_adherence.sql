MODEL (
  name core.yearly_category_adherence,
  kind FULL,
  grain (year, category_id),
  audits (
    not_null(columns := (year, category_id)),
    unique_combination_of_columns(columns := (year, category_id)),
    yearly_category_adherence_pct_of_bucket_sum
  ),
  description 'Per-year, per-category Needs/Wants adherence: net-spend signal (spend if any, else assigned), current-year extrapolation (spend YTD / elapsed months x 12), and each category share of its bucket projected spend.'
);

with category_years as (
    /*
        Per-category, per-year net activity and assigned dollars for the Needs
        and Wants buckets only. Investments adherence comes from
        core.yearly_investment_contributions; all other groups are excluded per
        the §5 mapping.

        Join monthly_categories.category_group_id directly to
        category_groups.id — hopping through cleaned.categories would pull the
        latest-snapshot group and mis-attribute historical months for any
        category that has moved between groups. The as-of-month affiliation
        lives on monthly_categories itself.

        activity_usd is negative for spend in YNAB; the next CTE flips the sign
        so a positive number is net outflow.

        Exclude budget months that have not elapsed yet: YNAB pre-creates
        future budget-month rows (zero activity) for scheduled/goal categories,
        which would inflate months_with_data and under-project the current year,
        and would also pull future assignments into the budgeted-fallback
        signal. Keeping only months up to the current month makes
        months_with_data the true count of elapsed months.
    */
    select
        monthly_categories.id as category_id
        , monthly_categories.year as year
        , max(monthly_categories.category_name) as category
        , category_groups.category_group_name_mapping as bucket
        , sum(monthly_categories.activity_usd) as activity_usd_total
        , sum(monthly_categories.budgeted_usd) as budgeted_usd_total
        , count(distinct monthly_categories.month) as months_with_data
    from cleaned.monthly_categories as monthly_categories
    inner join cleaned.category_groups as category_groups
        on monthly_categories.category_group_id = category_groups.id
    where category_groups.category_group_name_mapping in ('Needs', 'Wants')
        and coalesce(monthly_categories.deleted, false) = false
        and make_date(monthly_categories.year, monthly_categories.month, 1)
            <= date_trunc('month', current_date())
    group by 1, 2, 4
)

, signalled as (
    /*
        Year-level coalesce(spend, assigned): if the category had any activity,
        use absolute net spend; otherwise fall back to assigned (covers sinking
        funds that have not transacted yet). amount_spent is the same positive
        net spend, surfaced for the drilldown table.

        Drop categories whose yearly net activity is an inflow
        (activity_usd_total > 0, i.e. refunds exceed spend). Those would yield
        negative amount_spent / projected and distort pct_of_bucket; they are
        not meaningful "spend" against a Needs/Wants target. Net-spend
        (negative) and zero-activity (budgeted-fallback) categories are kept.

        Clamp the budgeted fallback at 0: a no-spend category can still net a
        negative yearly assignment when leftover dollars are pulled back out of
        it (a de-funding), which would otherwise make signal_year / projected
        negative. A no-spend category never "spent" below zero.
    */
    select
        category_id
        , year
        , category
        , bucket
        , months_with_data
        , year = cast(extract('year' from current_date()) as integer) as is_current_year
        , round(-1 * activity_usd_total, 2) as amount_spent
        , case
            when activity_usd_total <> 0 then round(-1 * activity_usd_total, 2)
            else greatest(0, round(budgeted_usd_total, 2))
        end as signal_year
    from category_years
    where activity_usd_total <= 0
)

, projected as (
    /*
        Current year with spend: extrapolate net spend across the elapsed months
        (distinct budget months present) to a full year. Current year without
        spend: fall back to signal_year (assigned), not extrapolated, per the §4
        signal rule. Past years: projected = actual signal_year.
    */
    select
        category_id
        , year
        , category
        , bucket
        , amount_spent
        , signal_year
        , months_with_data
        , is_current_year
        , case
            when is_current_year and amount_spent <> 0
                then round(amount_spent / months_with_data * 12, 2)
            else signal_year
        end as projected_current_year
    from signalled
)

select
    year
    , bucket
    , category
    , category_id
    , amount_spent
    , signal_year
    , months_with_data
    , projected_current_year
    , is_current_year as is_extrapolated
    , case
        when sum(projected_current_year) over (partition by year, bucket) = 0
            then null
        else round(
            projected_current_year
            / sum(projected_current_year) over (partition by year, bucket)
            , 4
        )
    end as pct_of_bucket
from projected
order by year desc, bucket, projected_current_year desc
