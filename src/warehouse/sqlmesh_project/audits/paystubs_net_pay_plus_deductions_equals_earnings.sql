AUDIT (
  name paystubs_net_pay_plus_deductions_equals_earnings
);

SELECT
  file_name,
  pay_period_start_date,
  net_pay_total,
  pre_tax_deductions_total,
  taxes_total,
  post_tax_deductions_total,
  net_pay_total + pre_tax_deductions_total + taxes_total + post_tax_deductions_total AS calculated_total,
  earnings_total
FROM @this_model
WHERE NOT (
  net_pay_total
  + pre_tax_deductions_total
  + taxes_total
  + post_tax_deductions_total = earnings_total
);
