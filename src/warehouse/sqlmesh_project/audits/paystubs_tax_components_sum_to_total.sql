AUDIT (
  name paystubs_tax_components_sum_to_total
);

SELECT
  file_name,
  pay_period_start_date,
  taxes_medicare_usd,
  taxes_federal_usd,
  taxes_state_usd,
  taxes_city_usd,
  taxes_nypfl_usd,
  taxes_disability_usd,
  taxes_social_security_usd,
  taxes_medicare_usd + taxes_federal_usd + taxes_state_usd + taxes_city_usd + taxes_nypfl_usd + taxes_disability_usd + taxes_social_security_usd AS calculated_total,
  taxes_total_usd
FROM @this_model
WHERE NOT (
  taxes_medicare_usd
  + taxes_federal_usd
  + taxes_state_usd
  + taxes_city_usd
  + taxes_nypfl_usd
  + taxes_disability_usd
  + taxes_social_security_usd = taxes_total_usd
);
