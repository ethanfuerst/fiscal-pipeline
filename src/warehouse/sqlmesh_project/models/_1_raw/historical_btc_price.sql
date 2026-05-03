MODEL (
  name raw.historical_btc_price,
  kind FULL,
  grain date,
  description 'Raw BTC 00:00 UTC price snapshots from CoinGecko /coins/bitcoin/history, partitioned in S3 by year/month. Hive partitioning surfaces year and month as columns.'
);

select * from @get_s3_partitioned_parquet_path('historical_btc_price')
