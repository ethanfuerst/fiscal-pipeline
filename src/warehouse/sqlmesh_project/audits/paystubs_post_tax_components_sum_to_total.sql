AUDIT (
  name paystubs_post_tax_components_sum_to_total
);

SELECT
  file_name,
  pay_period_start_date,
  post_tax_meal_allowance_offset_usd,
  post_tax_fitness_benefit_offset_usd,
  post_tax_roth_401k_usd,
  post_tax_401k_after_tax_spillover_usd,
  post_tax_401k_after_tax_bonus_usd,
  post_tax_critical_illness_usd,
  post_tax_ad_d_usd,
  post_tax_long_term_disability_usd,
  post_tax_citi_bike_usd,
  post_tax_meal_allowance_offset_usd + post_tax_fitness_benefit_offset_usd + post_tax_roth_401k_usd + post_tax_401k_after_tax_spillover_usd + post_tax_401k_after_tax_bonus_usd + post_tax_critical_illness_usd + post_tax_ad_d_usd + post_tax_long_term_disability_usd + post_tax_citi_bike_usd AS calculated_total,
  post_tax_deductions_total_usd
FROM @this_model
WHERE NOT (
  post_tax_meal_allowance_offset_usd
  + post_tax_fitness_benefit_offset_usd
  + post_tax_roth_401k_usd
  + post_tax_401k_after_tax_spillover_usd
  + post_tax_401k_after_tax_bonus_usd
  + post_tax_critical_illness_usd
  + post_tax_ad_d_usd
  + post_tax_long_term_disability_usd
  + post_tax_citi_bike_usd = post_tax_deductions_total_usd
);
