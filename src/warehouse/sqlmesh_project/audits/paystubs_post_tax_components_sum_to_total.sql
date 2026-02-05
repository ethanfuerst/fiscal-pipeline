AUDIT (
  name paystubs_post_tax_components_sum_to_total
);

SELECT
  file_name,
  pay_period_start_date,
  post_tax_meal_allowance_offset,
  post_tax_roth,
  post_tax_critical_illness,
  post_tax_ad_d,
  post_tax_long_term_disability,
  post_tax_citi_bike,
  post_tax_meal_allowance_offset + post_tax_roth + post_tax_critical_illness + post_tax_ad_d + post_tax_long_term_disability + post_tax_citi_bike AS calculated_total,
  post_tax_deductions_total
FROM @this_model
WHERE NOT (
  post_tax_meal_allowance_offset
  + post_tax_roth
  + post_tax_critical_illness
  + post_tax_ad_d
  + post_tax_long_term_disability
  + post_tax_citi_bike = post_tax_deductions_total
);
