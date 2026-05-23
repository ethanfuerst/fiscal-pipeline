MODEL (
  name cleaned.ticker_prices,
  kind FULL,
  grain (ticker, date),
  description 'Cleaned daily yfinance prices, one row per (ticker, trading date) where yfinance returned data. Tickers or dates absent here are reconstructed downstream from cleaned.investment_transactions buy/sell prices.'
);

select
    /* Primary key */
    cast(ticker as text) as ticker  -- ticker (S3 partition key)
    , cast(date as date) as date  -- trading date (business days only; no weekends/holidays)

    /* Money */
    , cast(open as decimal(18, 6)) as open_usd  -- open price (unadjusted)
    , cast(high as decimal(18, 6)) as high_usd  -- intraday high (unadjusted)
    , cast(low as decimal(18, 6)) as low_usd  -- intraday low (unadjusted)
    , cast(close as decimal(18, 6)) as close_usd  -- close price (unadjusted)
    , cast(adj_close as decimal(18, 6)) as adj_close_usd  -- close adjusted for dividends + splits

    /* Volume */
    , cast(coalesce(volume, 0) as bigint) as volume_shares  -- shares traded
from raw.ticker_prices
order by ticker, date
