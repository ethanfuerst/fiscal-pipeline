AUDIT (
  name yearly_investment_contributions_actual_sum
);

SELECT
  year,
  ytd_401k_contributions_usd,
  ytd_taxable_contributions_usd,
  investments_actual_usd,
  ytd_401k_contributions_usd + ytd_taxable_contributions_usd AS calculated_actual
FROM @this_model
WHERE NOT abs(investments_actual_usd - (ytd_401k_contributions_usd + ytd_taxable_contributions_usd)) < 0.02;
