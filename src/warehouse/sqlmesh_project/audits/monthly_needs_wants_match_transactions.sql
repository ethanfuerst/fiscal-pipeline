AUDIT (
  name monthly_needs_wants_match_transactions
);

WITH transaction_totals AS (
  SELECT
    DATE_TRUNC('month', transaction_date) AS budget_month,
    SUM(
      CASE
        WHEN category_group_name_mapping = 'Needs'
          THEN COALESCE(transaction_inflow, 0) - COALESCE(transaction_outflow, 0)
        ELSE 0
      END
    ) AS needs_spend,
    SUM(
      CASE
        WHEN category_group_name_mapping = 'Wants'
          THEN COALESCE(transaction_inflow, 0) - COALESCE(transaction_outflow, 0)
        ELSE 0
      END
    ) AS wants_spend
  FROM combined.transactions
  GROUP BY 1
),
dashboard AS (
  SELECT budget_month, needs_spend, wants_spend
  FROM @this_model
)
SELECT
  COALESCE(transaction_totals.budget_month, dashboard.budget_month) AS budget_month,
  transaction_totals.needs_spend AS source_needs_spend,
  dashboard.needs_spend AS dashboard_needs_spend,
  transaction_totals.wants_spend AS source_wants_spend,
  dashboard.wants_spend AS dashboard_wants_spend
FROM transaction_totals
FULL OUTER JOIN dashboard USING (budget_month)
WHERE ROUND(COALESCE(transaction_totals.needs_spend, 0), 2)
      != ROUND(COALESCE(dashboard.needs_spend, 0), 2)
  OR ROUND(COALESCE(transaction_totals.wants_spend, 0), 2)
      != ROUND(COALESCE(dashboard.wants_spend, 0), 2);
