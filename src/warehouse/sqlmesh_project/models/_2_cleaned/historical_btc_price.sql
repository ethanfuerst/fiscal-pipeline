MODEL (
  name cleaned.historical_btc_price,
  kind FULL,
  grain date,
  description 'Cleaned BTC/USD 00:00 UTC price, one row per UTC date from earliest wallet transaction date to today.'
);

select
    /* Primary key */
    cast(date as date) as date  -- UTC date

    /* Partition keys (from hive partitioning on raw parquet) */
    , cast(year as smallint) as year  -- UTC year (S3 partition key)
    , cast(month as smallint) as month  -- UTC month 1-12 (S3 partition key)
    , cast(day as smallint) as day  -- UTC day-of-month 1-31 (S3 partition key)

    /* Money */
    , price_usd  -- BTC price in USD at 00:00 UTC of the date (CoinGecko /coins/bitcoin/history)
from raw.historical_btc_price
order by date
