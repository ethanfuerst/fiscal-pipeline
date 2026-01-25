AUDIT (
  name paystubs_earnings_components_sum_to_total
);

SELECT
  file_name,
  pay_period_start_date,
  earnings_salary,
  earnings_bonus,
  earnings_meal_allowance,
  earnings_pto_payout,
  earnings_severance,
  earnings_misc,
  earnings_expense_reimbursement,
  earnings_nyc_citi_bike,
  earnings_salary + earnings_bonus + earnings_meal_allowance + earnings_pto_payout + earnings_severance + earnings_misc + earnings_expense_reimbursement + earnings_nyc_citi_bike AS calculated_total,
  earnings_total
FROM @this_model
WHERE NOT (
  earnings_salary
  + earnings_bonus
  + earnings_meal_allowance
  + earnings_pto_payout
  + earnings_severance
  + earnings_misc
  + earnings_expense_reimbursement
  + earnings_nyc_citi_bike = earnings_total
);
