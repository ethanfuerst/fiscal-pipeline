AUDIT (
  name paystubs_tax_components_sum_to_total
);

SELECT
  file_name,
  pay_period_start_date,
  taxes_medicare,
  taxes_federal,
  taxes_state,
  taxes_city,
  taxes_nypfl,
  taxes_disability,
  taxes_social_security,
  taxes_medicare + taxes_federal + taxes_state + taxes_city + taxes_nypfl + taxes_disability + taxes_social_security AS calculated_total,
  taxes_total
FROM @this_model
WHERE NOT (
  taxes_medicare
  + taxes_federal
  + taxes_state
  + taxes_city
  + taxes_nypfl
  + taxes_disability
  + taxes_social_security = taxes_total
);
