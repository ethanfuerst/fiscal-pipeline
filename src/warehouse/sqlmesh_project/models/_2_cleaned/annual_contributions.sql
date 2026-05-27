MODEL (
  name cleaned.annual_contributions,
  kind FULL,
  grain (year),
  description 'Per-year contribution targets: HSA contribution target, IRS 401(k) total annual additions limit, and IRS 401(k) employee elective deferral limit.'
);

select
    /* Primary key */
    cast(year as integer) as year  -- Calendar year these inputs apply to

    /* Money */
    , @try_cast_to_float(hsa_contribution_usd) as hsa_contribution_usd  -- HSA contribution target for the year, in USD
    , @try_cast_to_float(contribution_limit_401k_usd) as contribution_limit_401k_usd  -- IRS 401(k) total annual additions limit (employee + employer + after-tax combined) for the year, in USD (nullable when not yet set)
    , @try_cast_to_float(employee_contribution_limit_401k_usd) as employee_contribution_limit_401k_usd  -- IRS 401(k) employee elective deferral limit (pre-tax + Roth combined, employee only) for the year, in USD (nullable when not yet set)
from raw.annual_contributions
order by year
