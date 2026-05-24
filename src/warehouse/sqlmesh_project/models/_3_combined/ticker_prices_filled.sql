MODEL (
  name combined.ticker_prices_filled,
  kind FULL,
  grain (symbol, date),
  description 'Daily price per symbol: cleaned.ticker_prices.close_usd, forward-filled from investment_transactions.price for tickers yfinance has no data for (MMFs etc).'
);

with symbol_window as (
    select
        symbol
        , min(trade_date) as first_date
    from cleaned.investment_transactions
    where symbol is not null
        and symbol <> ''
        and trade_date is not null
    group by symbol
)

, txn_price_per_day as (
    select
        symbol
        , trade_date as date
        , avg(price) as txn_price
    from cleaned.investment_transactions
    where symbol is not null
        and symbol <> ''
        and trade_date is not null
        and price is not null
    group by symbol, trade_date
)

select
    days.date
    , sw.symbol
    , cast(
        coalesce(
            last_value(tp.close_usd ignore nulls) over (
                partition by sw.symbol
                order by days.date
                rows between unbounded preceding and current row
            )
            , last_value(txn.txn_price ignore nulls) over (
                partition by sw.symbol
                order by days.date
                rows between unbounded preceding and current row
            )
        ) as decimal(20, 6)
    ) as price_usd
    , tp.close_usd is not null as price_from_market
from symbol_window as sw
cross join combined.record_spine as days
left join cleaned.ticker_prices as tp
    on tp.ticker = sw.symbol
    and tp.date = days.date
left join txn_price_per_day as txn
    on txn.symbol = sw.symbol
    and txn.date = days.date
where days.date >= sw.first_date
order by sw.symbol, days.date
