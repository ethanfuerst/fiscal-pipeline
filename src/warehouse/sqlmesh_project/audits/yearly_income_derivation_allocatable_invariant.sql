AUDIT (
  name yearly_income_derivation_allocatable_invariant
);

SELECT
  year,
  salary,
  estimated_tax,
  hsa,
  allocatable_income,
  salary - (estimated_tax + hsa) AS calculated_allocatable
FROM @this_model
WHERE NOT abs(allocatable_income - (salary - (estimated_tax + hsa))) < 0.01;
