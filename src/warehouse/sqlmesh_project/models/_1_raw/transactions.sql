MODEL (
  name raw.transactions,
  kind FULL,
  grain id,
  description 'Raw YNAB transaction snapshots loaded directly from parquet, one row per transaction at last sync.'
);

select * from @get_s3_parquet_path('transactions')
