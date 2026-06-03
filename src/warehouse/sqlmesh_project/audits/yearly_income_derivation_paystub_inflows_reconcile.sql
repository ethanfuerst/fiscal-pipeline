AUDIT (
  name yearly_income_derivation_paystub_inflows_reconcile
);

/*
    Every Ready-to-Assign (`Income`) inflow linked to a paystub must reconcile
    to that paystub's net deposit (net_pay + reimbursements). extra_income is
    "total Income inflows minus salary paychecks"; if a paycheck under- or
    over-linked, the salary it removes would be wrong and the leftover extra
    would drift. net_pay lands once per paystub (daily_ledger nulls it on
    non-primary split rows), so summing per deposit-year reconciles cleanly.
*/
WITH linked_by_year AS (
  SELECT
    CAST(EXTRACT('year' FROM ledger_date) AS INTEGER) AS year,
    ROUND(SUM(transaction_amount_usd), 2) AS linked_inflow,
    ROUND(SUM(COALESCE(net_pay, 0) + COALESCE(income_for_reimbursements, 0)), 2) AS paystub_deposit
  FROM core.daily_ledger
  WHERE category_group_name_mapping = 'Income'
    AND paystub_file_name IS NOT NULL
  GROUP BY 1
)
SELECT
  m.year,
  l.linked_inflow,
  l.paystub_deposit,
  ROUND(l.linked_inflow - l.paystub_deposit, 2) AS diff
FROM @this_model AS m
JOIN linked_by_year AS l
  ON m.year = l.year
WHERE ROUND(l.linked_inflow, 2) != ROUND(l.paystub_deposit, 2);
