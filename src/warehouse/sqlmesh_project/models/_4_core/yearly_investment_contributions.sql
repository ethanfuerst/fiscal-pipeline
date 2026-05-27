MODEL (
  name core.yearly_investment_contributions,
  kind FULL,
  grain (year),
  audits (
    not_null(columns := (year)),
    unique_values(columns := (year)),
    yearly_investment_contributions_actual_sum,
    yearly_investment_contributions_split_targets_sum
  ),
  description 'Per-year YTD 401(k) and taxable brokerage contributions, with target split (50/50, 401(k) cap spillover), match-captured flag, and remaining-to-target.'
);

with paystubs_401k_ytd as (
    /*
        Per-year 401(k) contributions sourced from the canonical
        `retirement_fund_custom_calc_usd` rollup in cleaned.paystubs
        (pre-tax + Roth + after-tax spillover + after-tax bonus). That
        column is signed negative (deduction convention), so flip the
        sign to surface contributions as a positive figure. Past years
        are full-year totals; current year is YTD.
    */
    select
        cast(extract('year' from pay_date) as integer) as year
        , round(-1 * sum(retirement_fund_custom_calc_usd), 2) as ytd_401k_contributions_usd
    from cleaned.paystubs
    where pay_date is not null
    group by 1
)

, taxable_ytd as (
    /*
        YNAB outflows tagged to the Investments category group. activity_usd
        is negative for spending in YNAB; flip the sign so a positive number
        represents money sent to taxable brokerage. Net of any rare inflows
        back into Investments categories.

        Join `monthly_categories.category_group_id` directly to
        `category_groups.id` — using `cleaned.categories` as a hop would pull
        the latest snapshot's `category_group_id`, so historical months for
        any category that has moved between groups would resolve to the
        wrong bucket (or drop out of the inner join). The per-month
        affiliation lives on monthly_categories itself; use it.
    */
    select
        monthly_categories.year as year
        , round(-1 * sum(monthly_categories.activity_usd), 2) as ytd_taxable_contributions_usd
    from cleaned.monthly_categories as monthly_categories
    inner join cleaned.category_groups as category_groups
        on monthly_categories.category_group_id = category_groups.id
    where category_groups.category_group_name_mapping = 'Investments'
        and coalesce(monthly_categories.deleted, false) = false
    group by 1
)

, year_spine as (
    /*
        Years covered by either paystub or YNAB activity, derived from
        the central combined.record_spine. has_budget_data covers any
        YNAB transactions (which subsume Investments outflows);
        has_paystub_data covers paystubs (which income_derivation
        derives from).
    */
    select distinct cast(extract('year' from date) as integer) as year
    from combined.record_spine
    where has_paystub_data or has_budget_data
)

, joined as (
    select
        year_spine.year
        , year_spine.year = cast(extract('year' from current_date()) as integer) as is_current_year
        , coalesce(paystubs_401k_ytd.ytd_401k_contributions_usd, 0) as ytd_401k_contributions_usd
        , coalesce(taxable_ytd.ytd_taxable_contributions_usd, 0) as ytd_taxable_contributions_usd
        , annual_contributions.match_contribution_usd as match_contribution_usd
        , annual_contributions.contribution_limit_401k_usd as contribution_limit_401k_usd
        , yearly_income_derivation.allocatable_income as allocatable_income
        , yearly_income_derivation.investments_target as investments_target
    from year_spine
    left join paystubs_401k_ytd
        on year_spine.year = paystubs_401k_ytd.year
    left join taxable_ytd
        on year_spine.year = taxable_ytd.year
    left join cleaned.annual_contributions as annual_contributions
        on year_spine.year = annual_contributions.year
    left join core.yearly_income_derivation as yearly_income_derivation
        on year_spine.year = yearly_income_derivation.year
)

, with_split_targets as (
    /*
        50/50 split with 401(k) annual-limit spillover. When the limit is
        non-null and 0.50 × investments_target exceeds it, the cap binds
        and the remainder spills to taxable. When the limit is null (year
        not yet populated in annual_contributions), no cap is applied and
        the split stays a clean 50/50.
    */
    select
        joined.*
        , case
            when joined.investments_target is null
                then null
            when joined.contribution_limit_401k_usd is null
                then round(joined.investments_target * 0.50, 2)
            else round(
                least(joined.investments_target * 0.50, joined.contribution_limit_401k_usd)
                , 2
            )
        end as target_401k_split_usd
    from joined
)

select
    year
    , is_current_year

    /* Actuals */
    , ytd_401k_contributions_usd
    , ytd_taxable_contributions_usd
    , round(ytd_401k_contributions_usd + ytd_taxable_contributions_usd, 2) as investments_actual_usd

    /* Inputs from upstream */
    , match_contribution_usd
    , contribution_limit_401k_usd
    , allocatable_income
    , investments_target

    /* Derived metrics */
    , case
        when match_contribution_usd is null then null
        else ytd_401k_contributions_usd >= match_contribution_usd
    end as match_captured_flag
    , case
        when investments_target is null then null
        else round(
            greatest(
                0,
                investments_target - (ytd_401k_contributions_usd + ytd_taxable_contributions_usd)
            )
            , 2
        )
    end as investments_remaining_usd  -- Floored at 0: when actual ≥ target you are done, not "negative remaining"

    /* 50/50 split assessment with 401(k)-cap spillover */
    , target_401k_split_usd
    , case
        when investments_target is null or target_401k_split_usd is null then null
        else round(investments_target - target_401k_split_usd, 2)
    end as target_taxable_split_usd
    , case
        when (ytd_401k_contributions_usd + ytd_taxable_contributions_usd) = 0 then null
        else round(
            ytd_401k_contributions_usd
            / (ytd_401k_contributions_usd + ytd_taxable_contributions_usd)
            , 4
        )
    end as actual_401k_split_pct
    , case
        when (ytd_401k_contributions_usd + ytd_taxable_contributions_usd) = 0 then null
        else round(
            ytd_taxable_contributions_usd
            / (ytd_401k_contributions_usd + ytd_taxable_contributions_usd)
            , 4
        )
    end as actual_taxable_split_pct
from with_split_targets
order by year desc
