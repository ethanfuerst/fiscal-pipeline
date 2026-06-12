MODEL (
  name dashboards.income_derivation,
  kind FULL,
  grain (year),
  audits (
    not_null(columns := (year)),
    unique_values(columns := (year))
  ),
  description 'Tab 1 (ETH-472): §1 income derivation (salary, estimated tax, HSA, extra income, allocatable income, 50/30/15/5 targets) joined with the 401k match-milestone proxy (employee_limit_hit_flag) from core.yearly_investment_contributions and net_income (difference) from dashboards.yearly_level as the RED deficit conditional-format driver.'
);

select
    income.year
    , income.salary
    , income.estimated_tax
    , income.hsa
    , income.extra_income
    , income.allocatable_income
    , income.needs_target
    , income.wants_target
    , income.investments_target
    , income.savings_target
    , inv.employee_limit_hit_flag as match_milestone_hit  -- closest available proxy for "captured the full match"
    , inv.employee_contributions_remaining_usd
    , yl.difference as net_income  -- RED conditional-format driver (deficit when < 0)
    , income.is_extrapolated
from core.yearly_income_derivation as income
left join core.yearly_investment_contributions as inv
    on income.year = inv.year
left join dashboards.yearly_level as yl
    on income.year = yl.budget_year
order by income.year desc
