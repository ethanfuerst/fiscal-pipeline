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
  ),
  description 'Cleaned paystubs: dates parsed and money values cast from string to float.'
);

with cleaned_paystubs_int as (
    select
        /* Identifiers */
        file_name  -- Source paystub filename
        , employer  -- Employer name

        /* Timestamps */
        , pay_period_start_date as pay_period_start_date_raw  -- Pay period start, raw string from source
        , pay_period_end_date as pay_period_end_date_raw  -- Pay period end, raw string from source
        , pay_date as pay_date_raw  -- Pay date, raw string from source
        , @try_strip_date(pay_period_start_date) as pay_period_start_date  -- Parsed pay period start
        , @try_strip_date(pay_period_end_date) as pay_period_end_date  -- Parsed pay period end
        , @try_strip_date(pay_date) as pay_date  -- Parsed pay date

        /* Totals */
        , net_pay as net_pay_raw  -- Net pay total, raw string
        , earnings_total as earnings_total_raw  -- Gross earnings total, raw string
        , pre_tax_deductions as pre_tax_deductions_raw  -- Pre-tax deductions total, raw string
        , taxes as taxes_raw  -- Taxes total, raw string
        , post_tax_deductions as post_tax_deductions_raw  -- Post-tax deductions total, raw string
        , @try_cast_to_float(net_pay) as net_pay_total_usd  -- Net pay total in USD
        , @try_cast_to_float(earnings_total) as earnings_total_usd  -- Gross earnings total in USD
        , @try_cast_to_float(pre_tax_deductions) as pre_tax_deductions_total_usd  -- Pre-tax deductions total in USD
        , @try_cast_to_float(taxes) as taxes_total_usd  -- Taxes total in USD
        , @try_cast_to_float(post_tax_deductions) as post_tax_deductions_total_usd  -- Post-tax deductions total in USD

        /* Earnings components */
        , earnings_salary as earnings_salary_raw  -- Base salary, raw string
        , earnings_bonus as earnings_bonus_raw  -- Bonus, raw string
        , earnings_meal_allowance as earnings_meal_allowance_raw  -- Taxable meal allowance, raw string
        , earnings_fitness_benefit as earnings_fitness_benefit_raw  -- Taxable fitness benefit, raw string
        , earnings_pto_payout as earnings_pto_payout_raw  -- PTO payout, raw string
        , earnings_severance as earnings_severance_raw  -- Severance, raw string
        , earnings_misc as earnings_misc_raw  -- Miscellaneous earnings, raw string
        , earnings_expense_reimbursement as earnings_expense_reimbursement_raw  -- Non-taxable expense reimbursements, raw string
        , earnings_nyc_citi_bike as earnings_nyc_citi_bike_raw  -- NYC Citi Bike entry, raw string
        , @try_cast_to_float(earnings_salary) as earnings_salary_usd  -- Base salary in USD
        , @try_cast_to_float(earnings_bonus) as earnings_bonus_usd  -- Bonus in USD
        , @try_cast_to_float(earnings_meal_allowance) as earnings_meal_allowance_usd  -- Meal allowance (taxable) in USD
        , @try_cast_to_float(earnings_fitness_benefit) as earnings_fitness_benefit_usd  -- Fitness benefit (taxable) in USD
        , @try_cast_to_float(earnings_pto_payout) as earnings_pto_payout_usd  -- PTO payout in USD
        , @try_cast_to_float(earnings_severance) as earnings_severance_usd  -- Severance in USD
        , @try_cast_to_float(earnings_misc) as earnings_misc_usd  -- Miscellaneous earnings in USD
        , @try_cast_to_float(earnings_expense_reimbursement) as earnings_expense_reimbursement_usd  -- Expense reimbursements in USD
        , @try_cast_to_float(earnings_nyc_citi_bike) as earnings_nyc_citi_bike_usd  -- NYC Citi Bike entry in USD

        /* Pre-tax deductions */
        , pre_tax_401k as pre_tax_401k_raw  -- 401(k) contributions, raw string
        , pre_tax_hsa as pre_tax_hsa_raw  -- HSA contributions, raw string
        , pre_tax_fsa as pre_tax_fsa_raw  -- FSA contributions, raw string
        , pre_tax_medical as pre_tax_medical_raw  -- Medical premium, raw string
        , @try_cast_to_float(pre_tax_401k) as pre_tax_401k_usd  -- 401(k) contributions in USD
        , @try_cast_to_float(pre_tax_hsa) as pre_tax_hsa_usd  -- HSA contributions in USD
        , @try_cast_to_float(pre_tax_fsa) as pre_tax_fsa_usd  -- FSA contributions in USD
        , @try_cast_to_float(pre_tax_medical) as pre_tax_medical_usd  -- Medical premium in USD

        /* Taxes */
        , taxes_medicare as taxes_medicare_raw  -- Medicare tax, raw string
        , taxes_federal as taxes_federal_raw  -- Federal income tax, raw string
        , taxes_state as taxes_state_raw  -- State income tax, raw string
        , taxes_city as taxes_city_raw  -- City income tax, raw string
        , taxes_nypfl as taxes_nypfl_raw  -- NY Paid Family Leave, raw string
        , taxes_disability as taxes_disability_raw  -- State disability insurance, raw string
        , taxes_social_security as taxes_social_security_raw  -- Social Security tax, raw string
        , @try_cast_to_float(taxes_medicare) as taxes_medicare_usd  -- Medicare tax in USD
        , @try_cast_to_float(taxes_federal) as taxes_federal_usd  -- Federal income tax in USD
        , @try_cast_to_float(taxes_state) as taxes_state_usd  -- State income tax in USD
        , @try_cast_to_float(taxes_city) as taxes_city_usd  -- City income tax in USD
        , @try_cast_to_float(taxes_nypfl) as taxes_nypfl_usd  -- NY Paid Family Leave in USD
        , @try_cast_to_float(taxes_disability) as taxes_disability_usd  -- State disability insurance in USD
        , @try_cast_to_float(taxes_social_security) as taxes_social_security_usd  -- Social Security tax in USD

        /* Post-tax deductions */
        , post_tax_meal_allowance_offset as post_tax_meal_allowance_offset_raw  -- Offset for taxable meal allowance, raw string
        , post_tax_fitness_benefit_offset as post_tax_fitness_benefit_offset_raw  -- Offset for taxable fitness benefit, raw string
        , post_tax_roth as post_tax_roth_raw  -- Roth 401(k) contributions, raw string
        , post_tax_critical_illness as post_tax_critical_illness_raw  -- Critical illness premium, raw string
        , post_tax_ad_d as post_tax_ad_d_raw  -- AD&D premium, raw string
        , post_tax_long_term_disability as post_tax_long_term_disability_raw  -- Long-term disability premium, raw string
        , post_tax_citi_bike as post_tax_citi_bike_raw  -- Post-tax Citi Bike deduction, raw string
        , @try_cast_to_float(post_tax_meal_allowance_offset) as post_tax_meal_allowance_offset_usd  -- Meal allowance offset in USD
        , @try_cast_to_float(post_tax_fitness_benefit_offset) as post_tax_fitness_benefit_offset_usd  -- Fitness benefit offset in USD
        , @try_cast_to_float(post_tax_roth) as post_tax_roth_usd  -- Roth 401(k) contributions in USD
        , @try_cast_to_float(post_tax_critical_illness) as post_tax_critical_illness_usd  -- Critical illness premium in USD
        , @try_cast_to_float(post_tax_ad_d) as post_tax_ad_d_usd  -- AD&D premium in USD
        , @try_cast_to_float(post_tax_long_term_disability) as post_tax_long_term_disability_usd  -- Long-term disability premium in USD
        , @try_cast_to_float(post_tax_citi_bike) as post_tax_citi_bike_usd  -- Post-tax Citi Bike deduction in USD
    from raw.paystubs
)

, final as (
    select
        /* Identifiers and dates */
        file_name
        , employer
        , pay_period_start_date_raw
        , pay_period_end_date_raw
        , pay_date_raw
        , pay_period_start_date
        , pay_period_end_date
        , pay_date

        /* Raw money values */
        , net_pay_raw
        , earnings_total_raw
        , pre_tax_deductions_raw
        , taxes_raw
        , post_tax_deductions_raw
        , earnings_salary_raw
        , earnings_bonus_raw
        , earnings_meal_allowance_raw
        , earnings_fitness_benefit_raw
        , earnings_pto_payout_raw
        , earnings_severance_raw
        , earnings_misc_raw
        , earnings_expense_reimbursement_raw
        , earnings_nyc_citi_bike_raw
        , pre_tax_401k_raw
        , pre_tax_hsa_raw
        , pre_tax_fsa_raw
        , pre_tax_medical_raw
        , taxes_medicare_raw
        , taxes_federal_raw
        , taxes_state_raw
        , taxes_city_raw
        , taxes_nypfl_raw
        , taxes_disability_raw
        , taxes_social_security_raw
        , post_tax_meal_allowance_offset_raw
        , post_tax_fitness_benefit_offset_raw
        , post_tax_roth_raw
        , post_tax_critical_illness_raw
        , post_tax_ad_d_raw
        , post_tax_long_term_disability_raw
        , post_tax_citi_bike_raw

        /* USD values */
        , net_pay_total_usd
        , earnings_total_usd
        , pre_tax_deductions_total_usd
        , taxes_total_usd
        , post_tax_deductions_total_usd
        , earnings_salary_usd
        , earnings_bonus_usd
        , earnings_meal_allowance_usd
        , earnings_fitness_benefit_usd
        , earnings_pto_payout_usd
        , earnings_severance_usd
        , earnings_misc_usd
        , earnings_expense_reimbursement_usd
        , earnings_nyc_citi_bike_usd
        , pre_tax_401k_usd
        , pre_tax_hsa_usd
        , pre_tax_fsa_usd
        , pre_tax_medical_usd
        , taxes_medicare_usd
        , taxes_federal_usd
        , taxes_state_usd
        , taxes_city_usd
        , taxes_nypfl_usd
        , taxes_disability_usd
        , taxes_social_security_usd
        , post_tax_meal_allowance_offset_usd
        , post_tax_fitness_benefit_offset_usd
        , post_tax_roth_usd
        , post_tax_critical_illness_usd
        , post_tax_ad_d_usd
        , post_tax_long_term_disability_usd
        , post_tax_citi_bike_usd

        /* Custom calculated totals */
        , round(
            earnings_salary_usd
            + earnings_bonus_usd
            + earnings_pto_payout_usd
            + earnings_severance_usd
            , 2
        ) as earnings_custom_calc_usd  -- Salary + bonus + PTO payout + severance, in USD
        , round(earnings_bonus_usd + earnings_pto_payout_usd + earnings_severance_usd, 2) as bonus_custom_calc_usd  -- Bonus + PTO payout + severance, in USD
        , -1 * round(pre_tax_fsa_usd + pre_tax_medical_usd, 2) as pre_tax_deductions_custom_calc_usd  -- Pre-tax FSA + medical, signed negative, in USD
        , -1 * round(post_tax_roth_usd + pre_tax_401k_usd, 2) as retirement_fund_custom_calc_usd  -- Roth + 401(k), signed negative, in USD
        , -1 * round(
            taxes_medicare_usd
            + taxes_federal_usd
            + taxes_state_usd
            + taxes_city_usd
            + taxes_nypfl_usd
            + taxes_disability_usd
            + taxes_social_security_usd
            , 2
        ) as taxes_custom_calc_usd  -- All taxes summed, signed negative, in USD
        , -1 * pre_tax_hsa_usd as hsa_custom_calc_usd  -- HSA, signed negative, in USD
        , -1 * round(
            post_tax_critical_illness_usd
            + post_tax_ad_d_usd
            + post_tax_long_term_disability_usd
            , 2
        ) as post_tax_deductions_custom_calc_usd  -- Insurance premiums summed, signed negative, in USD
        , round(net_pay_total_usd - earnings_expense_reimbursement_usd, 2) as net_pay_custom_calc_usd  -- Net pay minus non-taxable reimbursements, in USD
    from cleaned_paystubs_int
)

select
    *
    , round(
        pre_tax_deductions_custom_calc_usd
        + taxes_custom_calc_usd
        + retirement_fund_custom_calc_usd
        + hsa_custom_calc_usd
        + post_tax_deductions_custom_calc_usd
        , 2
    ) as deductions_custom_calc_usd  -- Sum of all custom-calc deductions (signed negative), in USD
from final
