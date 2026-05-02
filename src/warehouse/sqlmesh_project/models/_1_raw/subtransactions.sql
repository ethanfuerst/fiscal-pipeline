MODEL (
  name raw.subtransactions,
  kind FULL,
  grain id,
  description 'Raw YNAB sub-transaction snapshots loaded directly from parquet — split components of split transactions.'
);

select * from @get_s3_parquet_path('subtransactions')
