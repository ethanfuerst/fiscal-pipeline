MODEL (
  name dashboards.investments,
  kind FULL,
  grain (year),
  audits (
    not_null(columns := (year)),
    unique_values(columns := (year))
  ),
  description 'Tab 3 (ETH-472): per-year 401(k)/taxable contribution actuals, limits, 50/50 split targets and actual split %, 15% target and remaining, §8 surplus to Investments, plus Emergency Fund top-ups from core.yearly_extra_income_allocation.'
);

select
    inv.year
    , inv.ytd_401k_contributions_usd
    , inv.ytd_401k_employee_contributions_usd
    , inv.ytd_taxable_contributions_usd
    , inv.investments_actual_usd
    , inv.contribution_limit_401k_usd
    , inv.employee_limit_hit_flag
    , inv.target_401k_split_usd
    , inv.target_taxable_split_usd
    , inv.actual_401k_split_pct
    , inv.actual_taxable_split_pct
    , inv.investments_target            -- 15% target
    , inv.investments_remaining_usd     -- remaining to hit 15%
    , inv.extra_income_surplus_to_investments
    , alloc.extra_income_used_for_emergency_fund_contributions as emergency_topups
    , inv.is_current_year as is_extrapolated
from core.yearly_investment_contributions as inv
left join core.yearly_extra_income_allocation as alloc
    on inv.year = alloc.year
order by inv.year desc
