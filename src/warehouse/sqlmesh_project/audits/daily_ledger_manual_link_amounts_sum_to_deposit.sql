AUDIT (
  name daily_ledger_manual_link_amounts_sum_to_deposit
);

WITH manual_sums AS (
  SELECT
    paystub_file_name,
    SUM(transaction_amount_usd) AS total_transaction_amount,
    MAX(COALESCE(net_pay, 0) + COALESCE(income_for_reimbursements, 0)) AS paystub_deposit
  FROM @this_model
  WHERE paystub_link_source = 'manual'
  GROUP BY 1
)
SELECT
  paystub_file_name,
  total_transaction_amount,
  paystub_deposit,
  ROUND(total_transaction_amount - paystub_deposit, 2) AS diff
FROM manual_sums
WHERE ROUND(total_transaction_amount, 2) != ROUND(paystub_deposit, 2);
