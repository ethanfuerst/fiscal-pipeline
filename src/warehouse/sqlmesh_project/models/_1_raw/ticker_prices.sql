MODEL (
  name raw.ticker_prices,
  kind FULL,
  grain (ticker, date),
  description 'Raw daily yfinance OHLCV + adj close, S3-partitioned by ticker.'
);

select * from @get_s3_partitioned_parquet_path('ticker_prices')
