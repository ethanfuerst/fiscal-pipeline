MODEL (
  name raw.paystubs,
  kind FULL,
  grain (file_name, pay_period_start_date),
  description 'Raw paystub data loaded directly from parquet (extracted from a Google Sheets source). All money and date columns are stored as strings and parsed/cast in cleaned.paystubs.'
);

select * from @get_s3_parquet_path('raw-paystubs')
