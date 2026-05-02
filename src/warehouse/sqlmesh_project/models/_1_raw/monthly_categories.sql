MODEL (
  name raw.monthly_categories,
  kind FULL,
  grain id,
  description 'Raw YNAB monthly category snapshots loaded directly from parquet, one row per category per month with budgeted/activity/balance and goal fields.'
);

select * from @get_s3_parquet_path('monthly-categories')
