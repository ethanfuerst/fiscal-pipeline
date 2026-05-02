MODEL (
  name raw.category_groups,
  kind FULL,
  grain id,
  description 'Raw YNAB category-group snapshots loaded directly from parquet.'
);

select * from @get_s3_parquet_path('category-groups')
