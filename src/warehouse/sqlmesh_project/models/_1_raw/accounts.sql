MODEL (
  name raw.accounts,
  kind FULL,
  grain id,
  description 'Raw YNAB account snapshots loaded directly from parquet, one row per account at last sync.'
);

select * from @get_s3_parquet_path('accounts')
