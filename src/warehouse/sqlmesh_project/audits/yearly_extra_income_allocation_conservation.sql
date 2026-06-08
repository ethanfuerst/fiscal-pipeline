AUDIT (
  name yearly_extra_income_allocation_conservation
);

SELECT
  year,
  extra_income,
  extra_income_used_for_needs_overage
    + extra_income_used_for_wants_overage
    + extra_income_used_for_emergency_fund_contributions
    + extra_income_used_for_savings_coverage
    + extra_income_surplus_to_investments AS allocated_total
FROM @this_model
WHERE NOT abs(
  (extra_income_used_for_needs_overage
   + extra_income_used_for_wants_overage
   + extra_income_used_for_emergency_fund_contributions
   + extra_income_used_for_savings_coverage
   + extra_income_surplus_to_investments) - extra_income
) < 0.02;
