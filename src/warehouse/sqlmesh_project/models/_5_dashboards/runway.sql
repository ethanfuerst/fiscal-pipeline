MODEL (
  name dashboards.runway,
  kind FULL,
  grain (year),
  audits (
    not_null(columns := (year)),
    unique_values(columns := (year))
  ),
  description 'Tab 5 (ETH-472): per-year runway snapshot — liquid cash, primary reserve (emergency_fund_balance), secondary reserve (hsa_reimbursable_reserve), the three runway views, worst runway, and zone. Passthrough of core.yearly_runway; the monthly sparkline series reads core.monthly_runway directly (documented exception).'
);

select
    year
    , liquid_cash
    , emergency_fund_balance         -- primary reserve (cash EF)
    , hsa_reimbursable_reserve       -- secondary reserve (HSA-reimbursable)
    , runway_trailing_3mo
    , runway_trailing_12mo
    , runway_projected
    , worst_runway
    , zone
    , is_extrapolated
from core.yearly_runway
order by year desc
