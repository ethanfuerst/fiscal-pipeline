MODEL (
  name cleaned.paystubs,
  kind FULL,
  grain (file_name, pay_period_start_date),
  audits (
    paystubs_earnings_components_sum_to_total,
    paystubs_pre_tax_components_sum_to_total,
    paystubs_tax_components_sum_to_total,
    paystubs_post_tax_components_sum_to_total,
    paystubs_net_pay_plus_deductions_equals_earnings
  )
);

with cleaned_paystubs_int as (
    select
        file_name
        , employer
        , @try_strip_date(pay_period_start_date) as pay_period_start_date
        , @try_strip_date(pay_period_end_date) as pay_period_end_date
        , @try_strip_date(pay_date) as pay_date
        , @try_cast_to_float(net_pay) as net_pay_total
        , @try_cast_to_float(earnings_total) as earnings_total
        , @try_cast_to_float(pre_tax_deductions) as pre_tax_deductions_total
        , @try_cast_to_float(taxes) as taxes_total
        , @try_cast_to_float(post_tax_deductions) as post_tax_deductions_total
        , @try_cast_to_float(earnings_salary) as earnings_salary
        , @try_cast_to_float(earnings_bonus) as earnings_bonus
        , @try_cast_to_float(earnings_meal_allowance) as earnings_meal_allowance
        , @try_cast_to_float(earnings_pto_payout) as earnings_pto_payout
        , @try_cast_to_float(earnings_severance) as earnings_severance
        , @try_cast_to_float(earnings_misc) as earnings_misc
        , @try_cast_to_float(earnings_expense_reimbursement) as earnings_expense_reimbursement
        , @try_cast_to_float(earnings_nyc_citi_bike) as earnings_nyc_citi_bike
        , @try_cast_to_float(pre_tax_401k) as pre_tax_401k
        , @try_cast_to_float(pre_tax_hsa) as pre_tax_hsa
        , @try_cast_to_float(pre_tax_fsa) as pre_tax_fsa
        , @try_cast_to_float(pre_tax_medical) as pre_tax_medical
        , @try_cast_to_float(taxes_medicare) as taxes_medicare
        , @try_cast_to_float(taxes_federal) as taxes_federal
        , @try_cast_to_float(taxes_state) as taxes_state
        , @try_cast_to_float(taxes_city) as taxes_city
        , @try_cast_to_float(taxes_nypfl) as taxes_nypfl
        , @try_cast_to_float(taxes_disability) as taxes_disability
        , @try_cast_to_float(taxes_social_security) as taxes_social_security
        , @try_cast_to_float(post_tax_meal_allowance_offset) as post_tax_meal_allowance_offset
        , @try_cast_to_float(post_tax_roth) as post_tax_roth
        , @try_cast_to_float(post_tax_critical_illness) as post_tax_critical_illness
        , @try_cast_to_float(post_tax_ad_d) as post_tax_ad_d
        , @try_cast_to_float(post_tax_long_term_disability) as post_tax_long_term_disability
        , @try_cast_to_float(post_tax_citi_bike) as post_tax_citi_bike
    from raw.paystubs
)

, final as (
    select
        file_name
        , employer
        , pay_period_start_date
        , pay_period_end_date
        , pay_date
        , net_pay_total
        , earnings_total
        , pre_tax_deductions_total
        , taxes_total
        , post_tax_deductions_total
        , earnings_salary
        , earnings_bonus
        , earnings_meal_allowance
        , earnings_pto_payout
        , earnings_severance
        , earnings_misc
        , earnings_expense_reimbursement
        , earnings_nyc_citi_bike
        , pre_tax_401k
        , pre_tax_hsa
        , pre_tax_fsa
        , pre_tax_medical
        , taxes_medicare
        , taxes_federal
        , taxes_state
        , taxes_city
        , taxes_nypfl
        , taxes_disability
        , taxes_social_security
        , post_tax_meal_allowance_offset
        , post_tax_roth
        , post_tax_critical_illness
        , post_tax_ad_d
        , post_tax_long_term_disability
        , post_tax_citi_bike
        , round(
            earnings_salary
            + earnings_bonus
            + earnings_pto_payout
            + earnings_severance
            , 2
        ) as earnings_custom_calc
        , round(earnings_bonus + earnings_pto_payout + earnings_severance, 2) as bonus_custom_calc
        , -1 * round(pre_tax_fsa + pre_tax_medical, 2) as pre_tax_deductions_custom_calc
        , -1 * round(post_tax_roth + pre_tax_401k, 2) as retirement_fund_custom_calc
        , -1 * round(
            taxes_medicare
            + taxes_federal
            + taxes_state
            + taxes_city
            + taxes_nypfl
            + taxes_disability
            + taxes_social_security
            , 2
        ) as taxes_custom_calc
        , -1 * pre_tax_hsa as hsa_custom_calc
        , -1 * round(
            post_tax_critical_illness
            + post_tax_ad_d
            + post_tax_long_term_disability
            , 2
        ) as post_tax_deductions_custom_calc
        , round(net_pay_total - earnings_expense_reimbursement, 2) as net_pay_custom_calc
    from cleaned_paystubs_int
)

select
    *
    , round(
        pre_tax_deductions_custom_calc
        + taxes_custom_calc
        + retirement_fund_custom_calc
        + hsa_custom_calc
        + post_tax_deductions_custom_calc
        , 2
    ) as deductions_custom_calc
from final
