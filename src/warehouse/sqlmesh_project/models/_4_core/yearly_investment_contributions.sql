MODEL (
  name core.yearly_investment_contributions,
  kind FULL,
  grain (year),
  audits (
    not_null(columns := (year)),
    unique_values(columns := (year)),
    yearly_investment_contributions_actual_sum,
    yearly_investment_contributions_split_targets_sum,
    yearly_investment_contributions_surplus_split_targets_sum,
    yearly_investment_contributions_surplus_on_plan_consistency
  ),
  description 'Per-year YTD 401(k) and taxable brokerage contributions, with target split (50/50, employee elective-limit spillover), employee-limit hit flag, and remaining-to-target. Also emits the §8 surplus-inclusive view (ETH-470 step 4): the extra-income surplus from core.yearly_extra_income_allocation augments the salary-based investments_target, re-split 50/50 with the same employee-limit spillover (*_with_surplus_usd columns). Salary-only target/split columns are unchanged. investments_on_plan_after_surplus_flag (more-is-good) is the Investments member of the per-bucket on-plan-after-allocation set whose Needs/Wants/Savings flags live in core.yearly_extra_income_allocation.'
);

with paystubs_401k_ytd as (
    /*
        Per-year 401(k) contributions. `ytd_401k_contributions_usd` uses
        the canonical `retirement_fund_custom_calc_usd` rollup in
        cleaned.paystubs (pre-tax + Roth + after-tax spillover + after-tax
        bonus, signed negative for deduction convention) and flips the
        sign. `ytd_401k_employee_contributions_usd` is the elective-
        deferral subset (pre-tax + Roth only) used to track progress
        against the IRS employee contribution limit. Past years are full-
        year totals; current year is YTD.
    */
    select
        cast(extract('year' from pay_date) as integer) as year
        , round(-1 * sum(retirement_fund_custom_calc_usd), 2) as ytd_401k_contributions_usd
        , round(sum(pre_tax_401k_usd + post_tax_roth_401k_usd), 2) as ytd_401k_employee_contributions_usd
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
        , coalesce(paystubs_401k_ytd.ytd_401k_employee_contributions_usd, 0) as ytd_401k_employee_contributions_usd
        , coalesce(taxable_ytd.ytd_taxable_contributions_usd, 0) as ytd_taxable_contributions_usd
        , annual_contributions.contribution_limit_401k_usd as contribution_limit_401k_usd
        , annual_contributions.employee_contribution_limit_401k_usd as employee_contribution_limit_401k_usd
        , yearly_income_derivation.allocatable_income as allocatable_income
        , yearly_income_derivation.investments_target as investments_target
        , coalesce(extra_allocation.extra_income_surplus_to_investments, 0) as extra_income_surplus_to_investments
    from year_spine
    left join paystubs_401k_ytd
        on year_spine.year = paystubs_401k_ytd.year
    left join taxable_ytd
        on year_spine.year = taxable_ytd.year
    left join cleaned.annual_contributions as annual_contributions
        on year_spine.year = annual_contributions.year
    left join core.yearly_income_derivation as yearly_income_derivation
        on year_spine.year = yearly_income_derivation.year
    left join core.yearly_extra_income_allocation as extra_allocation
        on year_spine.year = extra_allocation.year
)

, with_split_targets as (
    /*
        50/50 split with 401(k) employee-limit spillover. When the IRS
        employee elective-deferral limit is set and 0.50 × investments_target
        exceeds it, the cap binds and the remainder spills to taxable. When
        the limit is null (year not yet populated in annual_contributions),
        no cap is applied and the split stays a clean 50/50. The total
        annual-additions limit (contribution_limit_401k_usd) is tracked
        separately but does not drive spillover.
    */
    select
        joined.*
        , case
            when joined.investments_target is null
                then null
            when joined.employee_contribution_limit_401k_usd is null
                then round(joined.investments_target * 0.50, 2)
            else round(
                least(joined.investments_target * 0.50, joined.employee_contribution_limit_401k_usd)
                , 2
            )
        end as target_401k_split_usd
        /*
            §8 surplus wired into the split (ETH-470 step 4). The §8 extra-income
            surplus (core.yearly_extra_income_allocation) augments the salary-based
            investments_target; the same 50/50 employee-limit spillover is re-applied
            to the augmented target so extra income flows into the §3 split with cap
            spillover. Base salary-only columns above are left intact.
        */
        , case
            when joined.investments_target is null
                then null
            else round(joined.investments_target + joined.extra_income_surplus_to_investments, 2)
        end as investments_target_with_surplus_usd
        , case
            when joined.investments_target is null
                then null
            when joined.employee_contribution_limit_401k_usd is null
                then round((joined.investments_target + joined.extra_income_surplus_to_investments) * 0.50, 2)
            else round(
                least(
                    (joined.investments_target + joined.extra_income_surplus_to_investments) * 0.50,
                    joined.employee_contribution_limit_401k_usd
                )
                , 2
            )
        end as target_401k_split_with_surplus_usd
    from joined
)

select
    year
    , is_current_year

    /* Actuals */
    , ytd_401k_contributions_usd
    , ytd_401k_employee_contributions_usd
    , ytd_taxable_contributions_usd
    , round(ytd_401k_contributions_usd + ytd_taxable_contributions_usd, 2) as investments_actual_usd

    /* Inputs from upstream */
    , contribution_limit_401k_usd
    , employee_contribution_limit_401k_usd
    , allocatable_income
    , investments_target

    /* Derived metrics */
    , case
        when employee_contribution_limit_401k_usd is null then null
        else ytd_401k_employee_contributions_usd >= employee_contribution_limit_401k_usd
    end as employee_limit_hit_flag
    , case
        when employee_contribution_limit_401k_usd is null then null
        else round(
            greatest(0, employee_contribution_limit_401k_usd - ytd_401k_employee_contributions_usd)
            , 2
        )
    end as employee_contributions_remaining_usd  -- Floored at 0: when employee YTD ≥ limit you are done
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

    /* 50/50 split assessment with employee-limit spillover */
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

    /* §8 extra-income surplus wired into the split (ETH-470 step 4) */
    , extra_income_surplus_to_investments
    , investments_target_with_surplus_usd
    , target_401k_split_with_surplus_usd
    , case
        when investments_target_with_surplus_usd is null or target_401k_split_with_surplus_usd is null then null
        else round(investments_target_with_surplus_usd - target_401k_split_with_surplus_usd, 2)
    end as target_taxable_split_with_surplus_usd
    , case
        when investments_target_with_surplus_usd is null then null
        else round(
            greatest(
                0,
                investments_target_with_surplus_usd - (ytd_401k_contributions_usd + ytd_taxable_contributions_usd)
            )
            , 2
        )
    end as investments_remaining_with_surplus_usd  -- Floored at 0, mirrors investments_remaining_usd
    /*
        On-plan-after-surplus flag (more-is-good): actual contributions meet the
        surplus-augmented target. Parallels the per-bucket on-plan-after-allocation
        flags in core.yearly_extra_income_allocation; the Investments flag lives
        here because the actuals do (importing them into the allocation model would
        form a circular dependency).
    */
    , case
        when investments_target_with_surplus_usd is null then null
        else (ytd_401k_contributions_usd + ytd_taxable_contributions_usd) >= investments_target_with_surplus_usd
    end as investments_on_plan_after_surplus_flag
from with_split_targets
order by year desc
