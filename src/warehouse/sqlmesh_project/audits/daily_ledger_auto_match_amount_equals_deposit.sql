AUDIT (
  name daily_ledger_auto_match_amount_equals_deposit
);

SELECT
  transaction_id,
  paystub_file_name,
  ledger_date,
  transaction_amount_usd,
  net_pay,
  income_for_reimbursements,
  round(
    transaction_amount_usd - (coalesce(net_pay, 0) + coalesce(income_for_reimbursements, 0)),
    2
  ) AS diff
FROM @this_model
WHERE paystub_link_source = 'auto'
  AND round(transaction_amount_usd, 2)
      != round(coalesce(net_pay, 0) + coalesce(income_for_reimbursements, 0), 2);
