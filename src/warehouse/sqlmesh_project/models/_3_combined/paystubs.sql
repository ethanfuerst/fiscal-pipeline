MODEL (
  name combined.paystubs,
  kind FULL,
  grain pay_date
);

select
    pay_date
    , round(earnings_custom_calc, 2) as earnings_actual
    , round(pre_tax_deductions_custom_calc, 2) as pre_tax_deductions
    , round(taxes_custom_calc, 2) as taxes
    , round(retirement_fund_custom_calc, 2) as retirement_fund
    , round(hsa_custom_calc, 2) as hsa
    , round(post_tax_deductions_custom_calc, 2) as post_tax_deductions
    , round(deductions_custom_calc, 2) as deductions
    , round(net_pay_custom_calc, 2) as net_pay
    , round(earnings_expense_reimbursement, 2) as income_for_reimbursements
    , round(earnings_salary, 2) as salary
    , round(bonus_custom_calc, 2) as bonus
from cleaned.paystubs
order by pay_date desc
