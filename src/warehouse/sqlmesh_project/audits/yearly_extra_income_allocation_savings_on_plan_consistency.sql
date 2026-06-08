AUDIT (
  name yearly_extra_income_allocation_savings_on_plan_consistency
);

SELECT
  m.year,
  m.net_saved_after_coverage,
  savings.target AS savings_target,
  m.savings_on_plan_after_coverage
FROM @this_model AS m
LEFT JOIN core.yearly_savings_adherence AS savings
  ON m.year = savings.year
WHERE savings.target IS NOT NULL
  AND m.savings_on_plan_after_coverage <> (m.net_saved_after_coverage >= savings.target);
