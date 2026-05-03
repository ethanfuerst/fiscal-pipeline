MODEL (
  name raw.btc_wallet_history,
  kind FULL,
  grain (wallet_address, txid),
  description 'Raw BTC wallet transaction snapshots loaded directly from parquet, one row per (wallet_address, txid) at last sync.'
);

select * from @get_s3_parquet_path('btc-wallet-history')
