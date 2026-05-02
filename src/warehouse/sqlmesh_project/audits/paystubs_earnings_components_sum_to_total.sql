AUDIT (
  name paystubs_earnings_components_sum_to_total
);

SELECT
  file_name,
  pay_period_start_date,
  earnings_salary_usd,
  earnings_bonus_usd,
  earnings_meal_allowance_usd,
  earnings_pto_payout_usd,
  earnings_severance_usd,
  earnings_misc_usd,
  earnings_expense_reimbursement_usd,
  earnings_nyc_citi_bike_usd,
  earnings_salary_usd + earnings_bonus_usd + earnings_meal_allowance_usd + earnings_pto_payout_usd + earnings_severance_usd + earnings_misc_usd + earnings_expense_reimbursement_usd + earnings_nyc_citi_bike_usd AS calculated_total,
  earnings_total_usd
FROM @this_model
WHERE NOT (
  earnings_salary_usd
  + earnings_bonus_usd
  + earnings_meal_allowance_usd
  + earnings_pto_payout_usd
  + earnings_severance_usd
  + earnings_misc_usd
  + earnings_expense_reimbursement_usd
  + earnings_nyc_citi_bike_usd = earnings_total_usd
);
