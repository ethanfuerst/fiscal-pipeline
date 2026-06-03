AUDIT (
  name yearly_income_derivation_bucket_targets_sum
);

SELECT
  year,
  needs_target,
  wants_target,
  investments_target,
  savings_target,
  allocatable_income,
  needs_target + wants_target + investments_target + savings_target AS calculated_total
FROM @this_model
WHERE NOT abs((needs_target + wants_target + investments_target + savings_target) - allocatable_income) < 0.02;
