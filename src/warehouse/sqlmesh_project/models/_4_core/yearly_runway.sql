MODEL (
  name core.yearly_runway,
  kind FULL,
  grain (year),
  audits (
    not_null(columns := (year)),
    unique_values(columns := (year)),
    yearly_runway_liquid_cash_conservation,
    yearly_runway_net_burn_floor,
    yearly_runway_runway_consistency,
    yearly_runway_zone_consistency
  ),
  description 'Per-year §7 runway = the furthest-point slice of core.monthly_runway: the latest elapsed month of each year (December for past years, the current month for the current year). Same columns as the monthly model (gross-spend net burn, per-view runway, worst-view zone, emergency-fund + HSA backstops); ETH-472 reads this for the yearly KPI and core.monthly_runway for the sparkline.'
);

select
    cast(extract('year' from month) as integer) as year
    , bank_balance
    , emergency_fund_balance
    , savings_earmark
    , liquid_cash
    , hsa_reimbursable_reserve
    , net_burn_trailing_3mo
    , net_burn_trailing_12mo
    , net_burn_projected
    , gross_burn_trailing_3mo
    , gross_burn_trailing_12mo
    , runway_trailing_3mo
    , runway_trailing_12mo
    , runway_projected
    , gross_runway_trailing_3mo
    , gross_runway_trailing_12mo
    , worst_runway
    , zone
    , is_extrapolated
from core.monthly_runway
qualify row_number() over (
    partition by cast(extract('year' from month) as integer)
    order by month desc
) = 1
order by year desc
