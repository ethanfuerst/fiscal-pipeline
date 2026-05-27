MODEL (
  name raw.annual_contributions,
  kind FULL,
  grain (year),
  description 'Raw per-year contribution targets loaded from parquet.'
);

select * from @get_s3_parquet_path('raw-annual-contributions')
