MODEL (
  name cleaned.annual_contributions,
  kind FULL,
  grain (year),
  description 'Per-year contribution targets: employer 401(k) match, HSA contribution target, and IRS 401(k) total annual additions limit.'
);

select
    /* Primary key */
    cast(year as integer) as year  -- Calendar year these inputs apply to

    /* Money */
    , @try_cast_to_float(match_contribution_usd) as match_contribution_usd  -- Target employer 401(k) match for the year, in USD
    , @try_cast_to_float(hsa_contribution_usd) as hsa_contribution_usd  -- HSA contribution target for the year, in USD
    , @try_cast_to_float(contribution_limit_401k_usd) as contribution_limit_401k_usd  -- IRS 401(k) total annual additions limit (employee + employer + after-tax combined) for the year, in USD (nullable when not yet set)
from raw.annual_contributions
order by year
