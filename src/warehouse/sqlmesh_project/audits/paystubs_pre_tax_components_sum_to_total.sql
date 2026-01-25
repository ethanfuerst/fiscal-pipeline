AUDIT (
  name paystubs_pre_tax_components_sum_to_total
);

SELECT
  file_name,
  pay_period_start_date,
  pre_tax_401k,
  pre_tax_hsa,
  pre_tax_fsa,
  pre_tax_medical,
  pre_tax_401k + pre_tax_hsa + pre_tax_fsa + pre_tax_medical AS calculated_total,
  pre_tax_deductions_total
FROM @this_model
WHERE NOT (
  pre_tax_401k
  + pre_tax_hsa
  + pre_tax_fsa
  + pre_tax_medical = pre_tax_deductions_total
);
