MODEL (
  name combined.daily_positions,
  kind FULL,
  grain (broker, account_number, symbol, date),
  description 'Daily reconstructed position per (account, symbol). Cash is included as a synthetic position with symbol=NULL, is_cash=true, price_usd=1 — its quantity is the running cash balance. Security positions use cumulative qty_delta valued via combined.ticker_prices_filled.'
);

with security_deltas as (
    select
        broker
        , account_number
        , symbol
        , trade_date as date
        , sum(case
            when type in ('BUY', 'TRANSFER_IN', 'SPLIT') then coalesce(quantity, 0)
            when type in ('SELL', 'TRANSFER_OUT') then -coalesce(quantity, 0)
            else 0
        end) as qty_delta
    from cleaned.investment_transactions
    where trade_date is not null
        and symbol is not null
        and symbol <> ''
    group by broker, account_number, symbol, trade_date
)

, cash_deltas as (
    select
        broker
        , account_number
        , cast(null as text) as symbol
        , trade_date as date
        , sum(amount) as qty_delta
    from cleaned.investment_transactions
    where trade_date is not null
    group by broker, account_number, trade_date
)

, all_deltas as (
    select broker, account_number, symbol, date, qty_delta from security_deltas
    union all
    select broker, account_number, symbol, date, qty_delta from cash_deltas
)

, position_window as (
    select
        broker
        , account_number
        , symbol
        , min(date) as first_date
    from all_deltas
    group by broker, account_number, symbol
)

, running as (
    select
        days.date
        , pw.broker
        , pw.account_number
        , pw.symbol
        , pw.symbol is null as is_cash
        , cast(
            sum(coalesce(d.qty_delta, 0)) over (
                partition by pw.broker, pw.account_number, pw.symbol
                order by days.date
                rows between unbounded preceding and current row
            ) as decimal(38, 8)
        ) as quantity
    from position_window as pw
    cross join combined.record_spine as days
    left join all_deltas as d
        on d.broker = pw.broker
        and d.account_number = pw.account_number
        and d.symbol is not distinct from pw.symbol
        and d.date = days.date
    where days.date >= pw.first_date
)

select
    r.date
    , r.broker
    , r.account_number
    , r.symbol
    , r.is_cash
    , r.quantity
    , case when r.is_cash then cast(1 as decimal(20, 6)) else p.price_usd end as price_usd
    , cast(
        case when r.is_cash then r.quantity else r.quantity * p.price_usd end as decimal(24, 6)
    ) as market_value
from running as r
left join combined.ticker_prices_filled as p
    on p.symbol = r.symbol
    and p.date = r.date
order by r.broker, r.account_number, r.symbol nulls first, r.date
