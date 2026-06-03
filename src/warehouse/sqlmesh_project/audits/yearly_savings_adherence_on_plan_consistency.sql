AUDIT (
  name yearly_savings_adherence_on_plan_consistency
);

SELECT
  year,
  bucket,
  projected,
  target,
  on_plan_flag
FROM @this_model
WHERE target IS NOT NULL
  AND on_plan_flag <> (projected >= target);
