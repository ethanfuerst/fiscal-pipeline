MODEL (
  name raw.investment_transactions,
  kind FULL,
  grain (source_file_name, broker, account_number, trade_date, symbol, type, amount),
  description 'Raw investment transactions loaded directly from parquet.'
);

select * from @get_s3_parquet_path('raw-investment-transactions')
