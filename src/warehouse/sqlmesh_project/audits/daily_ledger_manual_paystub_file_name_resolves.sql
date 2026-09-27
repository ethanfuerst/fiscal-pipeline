AUDIT (
  name daily_ledger_manual_paystub_file_name_resolves
);

SELECT DISTINCT
  paystub_file_name,
  transaction_id,
  ledger_date,
  transaction_amount_usd
FROM @this_model
WHERE paystub_link_source = 'manual'
  AND paystub_file_name IS NOT NULL
  AND paystub_file_name NOT IN (
    SELECT file_name FROM combined.paystubs WHERE file_name IS NOT NULL
  );
