MODEL (
  name dashboards.extra_income_allocation,
  kind FULL,
  grain (year),
  audits (
    not_null(columns := (year)),
    unique_values(columns := (year))
  ),
  description 'Tab 2 §8 strip (ETH-472): per-year extra income and where it went (Emergency Fund contributions / Needs overage / Wants overage / Savings coverage / surplus to Investments). Thin passthrough of core.yearly_extra_income_allocation.'
);

select
    year
    , extra_income
    , extra_income_used_for_emergency_fund_contributions
    , extra_income_used_for_needs_overage
    , extra_income_used_for_wants_overage
    , extra_income_used_for_savings_coverage
    , extra_income_surplus_to_investments
    , is_extrapolated
from core.yearly_extra_income_allocation
order by year desc
