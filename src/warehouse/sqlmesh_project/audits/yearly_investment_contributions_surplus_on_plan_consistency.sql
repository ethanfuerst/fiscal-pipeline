AUDIT (
  name yearly_investment_contributions_surplus_on_plan_consistency
);

SELECT
  year,
  investments_actual_usd,
  investments_target_with_surplus_usd,
  investments_on_plan_after_surplus_flag
FROM @this_model
WHERE investments_target_with_surplus_usd IS NOT NULL
  AND investments_on_plan_after_surplus_flag
      <> (investments_actual_usd >= investments_target_with_surplus_usd);
