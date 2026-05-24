MODEL (
  name combined.daily_portfolio_value,
  kind FULL,
  grain (source_broker, account_number, date),
  description 'Daily total portfolio value per investment account: pivots combined.daily_positions into cash_balance (is_cash rows) + positions_market_value (non-cash rows) + total_value.'
);

select
    date
    , broker as source_broker
    , account_number
    , cast(sum(case when is_cash then coalesce(market_value, 0) else 0 end) as decimal(20, 4)) as cash_balance
    , cast(sum(case when not is_cash then coalesce(market_value, 0) else 0 end) as decimal(24, 6)) as positions_market_value
    , cast(sum(coalesce(market_value, 0)) as decimal(24, 6)) as total_value
from combined.daily_positions
group by date, broker, account_number
order by broker, account_number, date
