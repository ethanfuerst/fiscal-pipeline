AUDIT (
  name yearly_extra_income_allocation_overage_on_plan_consistency
);

/*
  Re-derive the Needs/Wants on-plan-after-allocation flags from the upstream
  bucket adherence model and confirm they match. projected/target are coalesced
  to 0 to mirror the model's `inputs` CTE; a 0 target means "no data" and the
  model emits a null flag, so those rows are excluded by the `<> 0` guards.
*/
SELECT
  m.year,
  needs.projected AS needs_projected,
  needs.target AS needs_target,
  m.extra_income_used_for_needs_overage,
  m.needs_on_plan_after_allocation,
  wants.projected AS wants_projected,
  wants.target AS wants_target,
  m.extra_income_used_for_wants_overage,
  m.wants_on_plan_after_allocation
FROM @this_model AS m
LEFT JOIN core.yearly_bucket_adherence AS needs
  ON m.year = needs.year AND needs.bucket = 'Needs'
LEFT JOIN core.yearly_bucket_adherence AS wants
  ON m.year = wants.year AND wants.bucket = 'Wants'
WHERE (
    coalesce(needs.target, 0) <> 0
    AND m.needs_on_plan_after_allocation
        <> ((coalesce(needs.projected, 0) - m.extra_income_used_for_needs_overage) <= needs.target)
  )
  OR (
    coalesce(wants.target, 0) <> 0
    AND m.wants_on_plan_after_allocation
        <> ((coalesce(wants.projected, 0) - m.extra_income_used_for_wants_overage) <= wants.target)
  );
