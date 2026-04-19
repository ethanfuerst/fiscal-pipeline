---
title: Yearly Overview
---

```sql overview
select
	budget_year
	, 'Pre-Tax Earnings' as value_type
	, earnings_actual as value
from yearly_level_dashboard

union all

select
	budget_year
	, 'Net Pay' as value_type
	, net_pay + ((retirement_fund + hsa) * -1) as value
from yearly_level_dashboard

union all

select
	budget_year
	, 'Spend' as value_type
	, spent * -1 as value
from yearly_level_dashboard
```

<BarChart
    data={overview}
    title="Overview"
    x=budget_year
    y=value
    yFmt=usd1k
    labels=true
    series=value_type
    type=grouped
/>

```sql all_income
select
    budget_year,
    'Pre Tax Earnings' as income_type,
    earnings_actual as income_amount,
from fiscal_pipeline.yearly_level_dashboard

union all

select
    budget_year,
    'Miscellaneous Income' as income_type,
    misc_income as income_amount,
from fiscal_pipeline.yearly_level_dashboard
```

<BarChart
    data={all_income}
    title="Income by year"
    x=budget_year
    y=income_amount
    yFmt=usd1k
    labels=true
    series=income_type
/>

```sql paycheck_breakdown
select
	budget_year
	, 'Net Pay' as breakdown_category
	, net_pay as amount
from yearly_level_dashboard

union all

select
	budget_year
	, 'Pre-Tax Deductions' as breakdown_category
	, pre_tax_deductions * -1 as amount
from yearly_level_dashboard

union all

select
	budget_year
	, 'Taxes' as breakdown_category
	, taxes * -1 as amount
from yearly_level_dashboard

union all

select
	budget_year
	, 'Retirement Fund Contribution' as breakdown_category
	, retirement_fund * -1 as amount
from yearly_level_dashboard

union all

select
	budget_year
	, 'HSA Contribution' as breakdown_category
	, hsa * -1 as amount
from yearly_level_dashboard

union all

select
	budget_year
	, 'Post-Tax Deductions' as breakdown_category
	, post_tax_deductions * -1 as amount
from yearly_level_dashboard
```

<BarChart
    data={paycheck_breakdown}
    title="Paycheck Breakdown by year"
    x=budget_year
    y=amount
    yFmt=usd1k
    labels=true
    series=breakdown_category
/>

```sql spending_breakdown

select
	budget_year
	, 'Needs' as spending_category
	, needs_spend as amount
from yearly_level_dashboard

union all

select
	budget_year
	, 'Wants' as spending_category
	, wants_spend as amount
from yearly_level_dashboard

union all

select
	budget_year
	, 'Savings' as spending_category
	, savings_spend as amount
from yearly_level_dashboard

union all

select
	budget_year
	, 'Emergency Fund' as spending_category
	, emergency_fund_spend as amount
from yearly_level_dashboard
```

<BarChart
    data={spending_breakdown}
    title="Spending by year"
    x=budget_year
    y=amount
    yFmt=usd1k
    labels=true
    series=spending_category
/>
