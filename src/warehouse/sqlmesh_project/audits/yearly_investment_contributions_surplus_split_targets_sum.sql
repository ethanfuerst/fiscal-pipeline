AUDIT (
  name yearly_investment_contributions_surplus_split_targets_sum
);

SELECT
  year,
  target_401k_split_with_surplus_usd,
  target_taxable_split_with_surplus_usd,
  investments_target_with_surplus_usd,
  target_401k_split_with_surplus_usd + target_taxable_split_with_surplus_usd AS calculated_total
FROM @this_model
WHERE investments_target_with_surplus_usd IS NOT NULL
  AND target_401k_split_with_surplus_usd IS NOT NULL
  AND target_taxable_split_with_surplus_usd IS NOT NULL
  AND NOT abs((target_401k_split_with_surplus_usd + target_taxable_split_with_surplus_usd) - investments_target_with_surplus_usd) < 0.02;
