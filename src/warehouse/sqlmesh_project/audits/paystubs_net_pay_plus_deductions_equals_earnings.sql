AUDIT (
  name paystubs_net_pay_plus_deductions_equals_earnings
);

SELECT
  file_name,
  pay_period_start_date,
  net_pay_total_usd,
  pre_tax_deductions_total_usd,
  taxes_total_usd,
  post_tax_deductions_total_usd,
  net_pay_total_usd + pre_tax_deductions_total_usd + taxes_total_usd + post_tax_deductions_total_usd AS calculated_total,
  earnings_total_usd
FROM @this_model
WHERE NOT (
  net_pay_total_usd
  + pre_tax_deductions_total_usd
  + taxes_total_usd
  + post_tax_deductions_total_usd = earnings_total_usd
);
