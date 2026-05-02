MODEL (
  name core.monthly_budgeted,
  kind FULL,
  grain budget_month,
  description 'Monthly-grain rollup of combined.monthly_budgeted: per-month assigned/balance totals for the savings/emergency-fund/investments/net-zero buckets that dashboards consume.'
);

select
    budget_month
    , sum(savings_assigned) as savings_assigned
    , sum(emergency_fund_assigned) as emergency_fund_assigned
    , sum(investments_assigned) as investments_assigned
    , sum(savings_balance) as savings_balance
    , sum(emergency_fund_balance) as emergency_fund_balance
    , sum(investments_balance) as investments_balance
    , sum(net_zero_balance) as net_zero_balance
from combined.monthly_budgeted
group by 1
order by budget_month desc
