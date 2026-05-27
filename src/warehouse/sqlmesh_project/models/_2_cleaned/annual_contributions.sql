MODEL (
  name cleaned.annual_contributions,
  kind FULL,
  grain (year),
  description 'Per-year contribution targets: employer 401(k) match and HSA contribution target.'
);

select
    /* Primary key */
    cast(year as integer) as year  -- Calendar year these inputs apply to

    /* Money */
    , @try_cast_to_float(match_contribution_usd) as match_contribution_usd  -- Target employer 401(k) match for the year, in USD
    , @try_cast_to_float(hsa_contribution_usd) as hsa_contribution_usd  -- HSA contribution target for the year, in USD
from raw.annual_contributions
order by year
