MODEL (
  name core.monthly_runway,
  kind FULL,
  grain (month),
  audits (
    not_null(columns := (month)),
    unique_values(columns := (month)),
    monthly_runway_liquid_cash_conservation,
    monthly_runway_net_burn_floor,
    monthly_runway_runway_consistency,
    monthly_runway_zone_consistency
  ),
  description 'Per-month §7 runway sparkline: as-of liquid cash (on-budget checking+savings ledger balance minus the emergency-fund balance and Savings assigned for that month) and burn views answering "if income stopped, how long does liquid cash cover spending" (no income subtraction). Trailing burn comes in two bases: net_burn_* (Needs+Wants spend NET of refunds, the canonical set that drives worst_runway / zone) and gross_burn_* (outflows only, refunds ignored, conservative). projected = §4 Needs+Wants projected / 12 (single, net basis). runway = liquid_cash / burn in months (NULL = N/A when burn is 0); worst-view zone over the net trailing+projected runways (Healthy >=2.00 / Watch 1.50-2.00 / Unhealthy <1.50; all-no-burn = Healthy). Also emits the emergency-fund and HSA-reimbursable backstops behind runway. Each month snapshots at month-end (today for the current month). History reconstructs balances from the raw ledger (cleaned.transactions drops transfers/starting-balance via its category filter). core.yearly_runway is the furthest-point slice of this model.'
);

with months as (
    /*
        Continuous month spine from the first budget month to the current month
        (record_spine is a gap-free daily spine), so the sparkline is unbroken.
    */
    select distinct date_trunc('month', date)::date as month
    from combined.record_spine
    where date >= (select min(date) from combined.record_spine where has_budget_data)
)

, month_anchors as (
    /*
        Per-month as-of date plus the last COMPLETE month that anchors the trailing
        windows. Completed months snapshot at month-end and include themselves in
        the window; the current (partial) month snapshots at today and anchors on
        the prior complete month so the partial month never deflates the rate.
        is_extrapolated marks current-year months (the projected view extrapolates).
    */
    select
        month
        , cast(extract('year' from month) as integer)
            = cast(extract('year' from current_date()) as integer) as is_extrapolated
        , case when month = date_trunc('month', current_date())::date then current_date()
               else (month + interval '1 month' - interval '1 day')::date end as as_of_date
        , case when month = date_trunc('month', current_date())::date
                   then (month - interval '1 month')::date
               else month end as anchor_month
    from months
)

, bank_accounts as (
    /*
        On-budget checking + savings accounts. Closed accounts are intentionally
        kept: a closed bank account's lifetime ledger nets to ~0 so the as-of
        reconstruction is unaffected, and excluding them would drop history for
        months the account was open and funded.
    */
    select id
    from cleaned.accounts
    where type in ('checking', 'savings')
      and is_on_budget
)

, bank_ledger as (
    /*
        Full RAW ledger for bank accounts. cleaned.transactions filters
        `category_id is not null`, which drops transfers and the Starting Balance
        row, so the running balance must come from raw. sum(amount) up to a date
        equals the YNAB working balance for that date (verified to the cent).
        raw.amount is milliunits as text; cast then /1000 for USD.
    */
    select
        cast(raw_txns.date as date) as txn_date
        , try_cast(raw_txns.amount as bigint) / 1000.0 as amount_usd
    from raw.transactions as raw_txns
    inner join bank_accounts
        on raw_txns.account_id = bank_accounts.id
    where coalesce(raw_txns.deleted, false) = false
)

, bank_balance_asof as (
    /* As-of bank balance per month: cumulative ledger up to that month's as-of date. */
    select
        month_anchors.month
        , round(coalesce(sum(bank_ledger.amount_usd), 0), 2) as bank_balance
    from month_anchors
    left join bank_ledger
        on bank_ledger.txn_date <= month_anchors.as_of_date
    group by 1
)

, earmarks as (
    /*
        Reserves that sit behind runway, for the month: emergency-fund running
        balance and Savings group assigned (budgeted). core.monthly_budgeted
        already rolls these per budget_month.
    */
    select
        month_anchors.month
        , coalesce(monthly_budgeted.emergency_fund_balance, 0) as emergency_fund_balance
        , coalesce(monthly_budgeted.savings_assigned, 0) as savings_earmark
    from month_anchors
    left join core.monthly_budgeted as monthly_budgeted
        on monthly_budgeted.budget_month = month_anchors.month
)

, hsa_monthly as (
    /*
        §6 secondary reserve source: monthly net activity of the HSA-reimbursement
        category (in Net Zero Expenses). Matched on name (ilike, emoji-tolerant);
        must NOT catch 'Misc. Transfers and Reimbursements'.
    */
    select
        make_date(monthly_categories.year, monthly_categories.month, 1) as cat_month
        , monthly_categories.activity_usd
    from cleaned.monthly_categories as monthly_categories
    inner join cleaned.category_groups as category_groups
        on monthly_categories.category_group_id = category_groups.id
    where category_groups.category_group_name_mapping = 'Net Zero Expenses'
      and monthly_categories.category_name ilike '%HSA Items for Reimbursement%'
      and coalesce(monthly_categories.deleted, false) = false
)

, hsa_reserve as (
    /* Lifetime |net activity| up to and including each month. Display only. */
    select
        month_anchors.month
        , round(abs(coalesce(sum(hsa_monthly.activity_usd), 0)), 2) as hsa_reimbursable_reserve
    from month_anchors
    left join hsa_monthly
        on hsa_monthly.cat_month <= month_anchors.month
    group by 1
)

, ledger_spend_monthly as (
    /*
        Per-calendar-month Needs+Wants spend from the core ledger, two bases. No
        income subtraction in either: runway is "if income stopped".
          - spend: NET of refunds (-1 x net activity; a refund reduces it). Drives
            the canonical net_burn / runway / zone (refunds are real cash back).
          - gross_spend: outflows only (refunds ignored). The conservative view.
        Orphan paystub rows have NULL mapping / amount and drop out.
    */
    select
        date_trunc('month', ledger_date)::date as flow_month
        , round(sum(case when category_group_name_mapping in ('Needs', 'Wants')
                         then -1 * transaction_amount_usd else 0 end), 2) as spend
        , round(sum(case when category_group_name_mapping in ('Needs', 'Wants')
                          and transaction_amount_usd < 0
                         then -1 * transaction_amount_usd else 0 end), 2) as gross_spend
    from core.daily_ledger
    where transaction_amount_usd is not null
    group by 1
)

, trailing as (
    /*
        Window monthly spend against each month's anchor. The 12mo window is the 12
        complete months ending at the anchor; the 3mo figure is the last 3 of those.
        greatest(0, spend) / window_months gives the zero-floored monthly burn.
    */
    select
        month_anchors.month
        , round(sum(case when ledger_spend_monthly.flow_month
                              > month_anchors.anchor_month - interval '3 months'
                         then ledger_spend_monthly.spend else 0 end), 2) as spend_3mo
        , round(sum(ledger_spend_monthly.spend), 2) as spend_12mo
        , round(sum(case when ledger_spend_monthly.flow_month
                              > month_anchors.anchor_month - interval '3 months'
                         then ledger_spend_monthly.gross_spend else 0 end), 2) as gross_spend_3mo
        , round(sum(ledger_spend_monthly.gross_spend), 2) as gross_spend_12mo
    from month_anchors
    left join ledger_spend_monthly
        on ledger_spend_monthly.flow_month
               > month_anchors.anchor_month - interval '12 months'
        and ledger_spend_monthly.flow_month <= month_anchors.anchor_month
    group by 1
)

, bucket_nw as (
    /* §4 projected outflows: Needs + Wants projected spend per year. */
    select year, round(sum(projected), 2) as nw_projected
    from core.yearly_bucket_adherence
    where bucket in ('Needs', 'Wants')
    group by 1
)

, assembled as (
    select
        month_anchors.month
        , month_anchors.is_extrapolated
        , bank_balance_asof.bank_balance
        , earmarks.emergency_fund_balance
        , earmarks.savings_earmark
        , hsa_reserve.hsa_reimbursable_reserve
        , round(
            bank_balance_asof.bank_balance
            - earmarks.emergency_fund_balance
            - earmarks.savings_earmark
          , 2) as liquid_cash
        , round(greatest(0, trailing.spend_3mo) / 3.0, 2) as net_burn_trailing_3mo
        , round(greatest(0, trailing.spend_12mo) / 12.0, 2) as net_burn_trailing_12mo
        , round(greatest(0, coalesce(bucket_nw.nw_projected, 0)) / 12.0, 2) as net_burn_projected
        , round(greatest(0, trailing.gross_spend_3mo) / 3.0, 2) as gross_burn_trailing_3mo
        , round(greatest(0, trailing.gross_spend_12mo) / 12.0, 2) as gross_burn_trailing_12mo
    from month_anchors
    left join bank_balance_asof on bank_balance_asof.month = month_anchors.month
    left join earmarks on earmarks.month = month_anchors.month
    left join hsa_reserve on hsa_reserve.month = month_anchors.month
    left join trailing on trailing.month = month_anchors.month
    left join bucket_nw
        on bucket_nw.year = cast(extract('year' from month_anchors.month) as integer)
)

, with_runway as (
    select
        assembled.*
        , case when net_burn_trailing_3mo = 0 then null
               else round(liquid_cash / net_burn_trailing_3mo, 2) end as runway_trailing_3mo
        , case when net_burn_trailing_12mo = 0 then null
               else round(liquid_cash / net_burn_trailing_12mo, 2) end as runway_trailing_12mo
        , case when net_burn_projected = 0 then null
               else round(liquid_cash / net_burn_projected, 2) end as runway_projected
        , case when gross_burn_trailing_3mo = 0 then null
               else round(liquid_cash / gross_burn_trailing_3mo, 2) end as gross_runway_trailing_3mo
        , case when gross_burn_trailing_12mo = 0 then null
               else round(liquid_cash / gross_burn_trailing_12mo, 2) end as gross_runway_trailing_12mo
    from assembled
)

, with_worst as (
    select
        with_runway.*
        /* least() ignores NULLs; NULL only when every view is no-burn. */
        , least(runway_trailing_3mo, runway_trailing_12mo, runway_projected) as worst_runway
    from with_runway
)

select
    month
    , bank_balance
    , emergency_fund_balance
    , savings_earmark
    , liquid_cash
    , hsa_reimbursable_reserve
    , net_burn_trailing_3mo
    , net_burn_trailing_12mo
    , net_burn_projected
    , gross_burn_trailing_3mo
    , gross_burn_trailing_12mo
    , runway_trailing_3mo
    , runway_trailing_12mo
    , runway_projected
    , gross_runway_trailing_3mo
    , gross_runway_trailing_12mo
    , worst_runway
    , case
        when worst_runway is null then 'Healthy'
        when worst_runway >= 2.00 then 'Healthy'
        when worst_runway >= 1.50 then 'Watch'
        else 'Unhealthy'
      end as zone
    , is_extrapolated
from with_worst
order by month desc
