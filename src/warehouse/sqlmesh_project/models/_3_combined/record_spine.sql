MODEL (
  name combined.record_spine,
  kind FULL,
  grain (date),
  description 'Daily date spine from the earliest user-event date across cleaned models to today, flagged with which source had activity that day. Downstream models join/aggregate and filter on the flags (e.g. monthly_level keeps only months where has_budget_data is true).'
);

with date_spine as (
    select cast(days.generate_series as date) as date
    from generate_series(
        least(
            (select min(transaction_date) from cleaned.transactions)
            , (select min(trade_date) from cleaned.investment_transactions)
            , (select min(budget_month) from cleaned.monthly_categories)
            , (select min(pay_date) from cleaned.paystubs)
            , (select min(transaction_date) from cleaned.btc_wallet_history)
        )::date
        , current_date::date
        , interval '1 day'
    ) as days
)

, budget_dates as (
    select distinct transaction_date as date
    from cleaned.transactions
    where transaction_date is not null
)

, investment_dates as (
    select distinct trade_date as date
    from cleaned.investment_transactions
    where trade_date is not null
)

, paystub_dates as (
    select distinct pay_date as date
    from cleaned.paystubs
    where pay_date is not null
)

select
    d.date
    , b.date is not null as has_budget_data
    , i.date is not null as has_investment_data
    , p.date is not null as has_paystub_data
from date_spine as d
left join budget_dates as b on b.date = d.date
left join investment_dates as i on i.date = d.date
left join paystub_dates as p on p.date = d.date
order by d.date
