---
title: Monthly Overview
---

```sql overview
select
	budget_month
	, 'Pre-Tax Earnings' as value_type
	, earnings_actual as value
from monthly_level_dashboard

union all

select
	budget_month
	, 'Net Pay' as value_type
	, net_pay + ((retirement_fund + hsa) * -1) as value
from monthly_level_dashboard

union all

select
	budget_month
	, 'Spend' as value_type
	, spent * -1 as value
from monthly_level_dashboard
```

<BarChart
    data={overview}
    title="Overview"
    x=budget_month
    y=value
    yFmt=usd1k
    series=value_type
    type=grouped
/>

```sql all_income
select
    budget_month,
    'Salary' as income_type,
    salary as income_amount,
from fiscal_pipeline.monthly_level_dashboard

union all

select
    budget_month,
    'Bonus' as income_type,
    bonus as income_amount,
from fiscal_pipeline.monthly_level_dashboard

union all

select
    budget_month,
    'Miscellaneous Income' as income_type,
    misc_income as income_amount,
from fiscal_pipeline.monthly_level_dashboard
```

<BarChart
    data={all_income}
    title="Income by Month"
    x=budget_month
    y=income_amount
    yFmt=usd1k
    series=income_type
/>

```sql paycheck_breakdown
select
	budget_month
	, 'Net Pay' as breakdown_category
	, net_pay as amount
from monthly_level_dashboard

union all

select
	budget_month
	, 'Pre-Tax Deductions' as breakdown_category
	, pre_tax_deductions * -1 as amount
from monthly_level_dashboard

union all

select
	budget_month
	, 'Taxes' as breakdown_category
	, taxes * -1 as amount
from monthly_level_dashboard

union all

select
	budget_month
	, 'Retirement Fund Contribution' as breakdown_category
	, retirement_fund * -1 as amount
from monthly_level_dashboard

union all

select
	budget_month
	, 'HSA Contribution' as breakdown_category
	, hsa * -1 as amount
from monthly_level_dashboard

union all

select
	budget_month
	, 'Post-Tax Deductions' as breakdown_category
	, post_tax_deductions * -1 as amount
from monthly_level_dashboard
```

<BarChart
    data={paycheck_breakdown}
    title="Paycheck Breakdown by Month"
    x=budget_month
    y=amount
    yFmt=usd1k
    series=breakdown_category
/>

```sql spending_breakdown

select
	budget_month
	, 'Needs' as spending_category
	, needs_spend as amount
from monthly_level_dashboard

union all

select
	budget_month
	, 'Wants' as spending_category
	, wants_spend as amount
from monthly_level_dashboard

union all

select
	budget_month
	, 'Savings' as spending_category
	, savings_spend as amount
from monthly_level_dashboard

union all

select
	budget_month
	, 'Emergency Fund' as spending_category
	, emergency_fund_spend as amount
from monthly_level_dashboard
```

<BarChart
    data={spending_breakdown}
    title="Spending by Month"
    x=budget_month
    y=amount
    yFmt=usd1k
    series=spending_category
/>
