AUDIT (
  name yearly_investment_contributions_split_targets_sum
);

SELECT
  year,
  target_401k_split_usd,
  target_taxable_split_usd,
  investments_target,
  target_401k_split_usd + target_taxable_split_usd AS calculated_total
FROM @this_model
WHERE investments_target IS NOT NULL
  AND target_401k_split_usd IS NOT NULL
  AND target_taxable_split_usd IS NOT NULL
  AND NOT abs((target_401k_split_usd + target_taxable_split_usd) - investments_target) < 0.02;
