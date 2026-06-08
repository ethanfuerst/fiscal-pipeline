AUDIT (
  name monthly_runway_liquid_cash_conservation
);

SELECT
  month,
  liquid_cash,
  bank_balance,
  emergency_fund_balance,
  savings_earmark
FROM @this_model
WHERE NOT abs(
  liquid_cash - (bank_balance - emergency_fund_balance - savings_earmark)
) < 0.01;
