MODEL (
  name core.yearly_income_derivation,
  kind FULL,
  grain (year),
  audits (
    yearly_income_derivation_allocatable_invariant,
    yearly_income_derivation_bucket_targets_sum,
    yearly_income_derivation_paystub_inflows_reconcile
  ),
  description 'Per-year salary, estimated tax (cadence-based extrapolation for the current year), HSA, allocatable income, 50/30/15/5 bucket targets (Needs/Wants/Investments/Savings), and extra_income (net-to-account one-off inflows: bonus/severance/PTO payout/refunds/interest/cashback, actual YTD, not extrapolated; does not feed allocatable_income).'
);

with paystub_periods as (
    /*
        Per-paystub rows with the gap to the previous paystub in the same
        year. gap_days drives the current-year cadence inference downstream.
        Filter to regular salary paystubs only: bonus-only paystubs lack a
        regular cadence and would distort the gap series.
    */
    select
        cast(extract('year' from pay_date) as integer) as year
        , pay_date
        , earnings_salary_usd
        , taxes_total_usd
        , pay_date::date - lag(pay_date::date) over (
            partition by cast(extract('year' from pay_date) as integer)
            order by pay_date
        ) as gap_days
    from cleaned.paystubs
    where earnings_salary_usd > 0
        and earnings_bonus_usd = 0
)

, paystub_years as (
    /*
        Per-year YTD raw sums, paystub count, the latest pay_date, and the
        average gap between consecutive pay_dates (NULL for years with a
        single paystub). avg(gap_days) ignores the NULL leading gap.
    */
    select
        year
        , sum(earnings_salary_usd) as salary_ytd
        , sum(taxes_total_usd) as estimated_tax_ytd
        , count(*) as paystubs_ytd
        , max(pay_date) as latest_pay_date
        , avg(gap_days) as avg_gap_days
    from paystub_periods
    group by 1
)

, flagged_years as (
    /*
        Add is_extrapolated, latest_pay_doy, and inferred_paystubs_per_year
        once so downstream CTEs branch on / divide by columns instead of
        recomputing. inferred_paystubs_per_year ≈ round(365 / avg_gap_days)
        gives the expected annual paystub count from the observed cadence
        (semi-monthly→24, biweekly→26, weekly→52, monthly→12). NULL when
        only one paystub exists (no gap to measure).
    */
    select
        year
        , salary_ytd
        , estimated_tax_ytd
        , paystubs_ytd
        , latest_pay_date
        , year = cast(extract('year' from current_date()) as integer) as is_extrapolated
        , extract('doy' from latest_pay_date) as latest_pay_doy
        , case
            when avg_gap_days is not null and avg_gap_days > 0
                then cast(round(365.0 / avg_gap_days) as integer)
        end as inferred_paystubs_per_year
    from paystub_years
)

, scaled as (
    /*
        Past years: pass YTD totals through as the actual value.
        Current year: extrapolate by paystub count using the inferred annual
        cadence as the divisor. If only one paystub exists so far (no gap
        to infer cadence from), fall back to the day-of-year formula.
    */
    select
        flagged_years.year
        , flagged_years.is_extrapolated
        , flagged_years.latest_pay_date
        , case
            when flagged_years.is_extrapolated and flagged_years.inferred_paystubs_per_year is not null
                then round(
                    flagged_years.salary_ytd
                    / flagged_years.paystubs_ytd
                    * flagged_years.inferred_paystubs_per_year,
                    2
                )
            when flagged_years.is_extrapolated
                then round(
                    flagged_years.salary_ytd / flagged_years.latest_pay_doy * 365,
                    2
                )
            else round(flagged_years.salary_ytd, 2)
        end as salary
        , case
            when flagged_years.is_extrapolated and flagged_years.inferred_paystubs_per_year is not null
                then round(
                    flagged_years.estimated_tax_ytd
                    / flagged_years.paystubs_ytd
                    * flagged_years.inferred_paystubs_per_year,
                    2
                )
            when flagged_years.is_extrapolated
                then round(
                    flagged_years.estimated_tax_ytd / flagged_years.latest_pay_doy * 365,
                    2
                )
            else round(flagged_years.estimated_tax_ytd, 2)
        end as estimated_tax
    from flagged_years
)

, allocatable as (
    /*
        Join annual_contributions for hsa and compute allocatable_income
        once. The final select then multiplies by 0.50 / 0.30 / 0.15 / 0.05
        for bucket targets without restating the subtraction.
    */
    select
        scaled.year
        , scaled.salary
        , scaled.estimated_tax
        , coalesce(annual_contributions.hsa_contribution_usd, 0)::decimal as hsa
        , round(
            scaled.salary
            - (scaled.estimated_tax + coalesce(annual_contributions.hsa_contribution_usd, 0))
            , 2
        ) as allocatable_income
        , scaled.is_extrapolated
        , scaled.latest_pay_date
    from scaled
    left join cleaned.annual_contributions as annual_contributions
        on scaled.year = annual_contributions.year
)

, extra_income_by_year as (
    /*
        Per-year extra income: every net inflow assigned to Ready-to-Assign
        (the YNAB `Income` mapping) that is NOT a regular-salary paycheck.
        core.daily_ledger links each paycheck deposit to its paystub, so
        excluding inflows linked to a salary paystub (earnings_salary_usd > 0)
        strips net salary without payee matching or a salary double-count.
        What remains is net-to-account one-off income: bonus, severance, PTO
        payout, tax refunds, interest, cashback, and side deposits. Actual YTD
        for the current year — one-offs are never extrapolated. Join on the
        unique paystub file_name (not daily_ledger's rank-gated salary column)
        so a split paycheck is still classified by its paystub type.
    */
    select
        cast(extract('year' from daily_ledger.ledger_date) as integer) as year
        , round(sum(daily_ledger.transaction_amount_usd), 2) as extra_income
    from core.daily_ledger as daily_ledger
    left join cleaned.paystubs as paystubs
        on daily_ledger.paystub_file_name = paystubs.file_name
    where daily_ledger.category_group_name_mapping = 'Income'
        and daily_ledger.transaction_amount_usd > 0
        and coalesce(paystubs.earnings_salary_usd, 0) = 0
    group by 1
)

select
    allocatable.year
    , allocatable.salary
    , allocatable.estimated_tax
    , allocatable.hsa
    , allocatable.allocatable_income
    , round(allocatable.allocatable_income * 0.50, 2) as needs_target
    , round(allocatable.allocatable_income * 0.30, 2) as wants_target
    , round(allocatable.allocatable_income * 0.15, 2) as investments_target
    , round(allocatable.allocatable_income * 0.05, 2) as savings_target
    , coalesce(extra_income_by_year.extra_income, 0) as extra_income
    , allocatable.is_extrapolated
    , allocatable.latest_pay_date
from allocatable
left join extra_income_by_year
    on allocatable.year = extra_income_by_year.year
order by allocatable.year desc
