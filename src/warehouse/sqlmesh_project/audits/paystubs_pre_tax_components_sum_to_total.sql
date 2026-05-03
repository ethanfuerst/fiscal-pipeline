AUDIT (
  name paystubs_pre_tax_components_sum_to_total
);

SELECT
  file_name,
  pay_period_start_date,
  pre_tax_401k_usd,
  pre_tax_hsa_usd,
  pre_tax_fsa_usd,
  pre_tax_medical_usd,
  pre_tax_401k_usd + pre_tax_hsa_usd + pre_tax_fsa_usd + pre_tax_medical_usd AS calculated_total,
  pre_tax_deductions_total_usd
FROM @this_model
WHERE NOT (
  pre_tax_401k_usd
  + pre_tax_hsa_usd
  + pre_tax_fsa_usd
  + pre_tax_medical_usd = pre_tax_deductions_total_usd
);
