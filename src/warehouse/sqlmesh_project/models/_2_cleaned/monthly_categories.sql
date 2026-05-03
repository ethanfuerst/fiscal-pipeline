MODEL (
  name cleaned.monthly_categories,
  kind FULL,
  grain id,
  description 'Cleaned YNAB monthly categories.'
);

select
    /* Primary key */
    id  -- YNAB category UUID (not unique across months — see cleaned.categories for a deduped dim)

    /* Foreign keys */
    , category_group_id  -- Parent category group UUID
    , original_category_group_id  -- Original category group UUID before any moves

    /* Timestamps */
    , month  -- Month number (1-12) of the budget period
    , year  -- Year of the budget period

    /* Status and properties */
    , name  -- Category display name (raw, may contain emojis or other noisy chars)
    , hidden  -- Whether the category is hidden in YNAB
    , deleted  -- Whether the category has been deleted
    , note  -- User-entered note (nullable)

    /* Goal configuration */
    , goal_type  -- Goal type (TB target balance, TBD target balance by date, MF monthly funding, NEED plan-your-spending, DEBT)
    , goal_needs_whole_amount  -- Whether the goal requires the whole amount to be funded
    , goal_day  -- Day of month/week the goal is due (nullable)
    , goal_cadence  -- Goal cadence (0..14, see YNAB API)
    , goal_cadence_frequency  -- Goal cadence frequency multiplier
    , goal_creation_month  -- Month the goal was created
    , goal_target_month  -- Target month the goal should be reached by (nullable)
    , goal_percentage_complete  -- Percent of goal funded (0-100)
    , goal_months_to_budget  -- Months remaining to fund the goal

    /* Money */
    , budgeted  -- Amount budgeted to the category for the month, milliunits
    , budgeted / 10 as budgeted_cents  -- Amount budgeted in cents
    , budgeted / 1000 as budgeted_usd  -- Amount budgeted in USD
    , activity  -- Net transaction activity for the month, milliunits
    , activity / 10 as activity_cents  -- Activity in cents
    , activity / 1000 as activity_usd  -- Activity in USD
    , balance  -- Available balance at end of month, milliunits
    , balance / 10 as balance_cents  -- Available balance in cents
    , balance / 1000 as balance_usd  -- Available balance in USD
    , goal_target  -- Goal target amount, milliunits
    , goal_target / 10 as goal_target_cents  -- Goal target in cents
    , goal_target / 1000 as goal_target_usd  -- Goal target in USD
    , goal_under_funded  -- Amount still needed to fully fund the goal this month, milliunits
    , goal_under_funded / 10 as goal_under_funded_cents  -- Goal funding needed this month, cents
    , goal_under_funded / 1000 as goal_under_funded_usd  -- Goal funding needed this month, USD
    , goal_overall_funded  -- Total funded toward the goal so far, milliunits
    , goal_overall_funded / 10 as goal_overall_funded_cents  -- Total funded in cents
    , goal_overall_funded / 1000 as goal_overall_funded_usd  -- Total funded in USD
    , goal_overall_left  -- Amount left to fully fund the goal overall, milliunits
    , goal_overall_left / 10 as goal_overall_left_cents  -- Goal remaining in cents
    , goal_overall_left / 1000 as goal_overall_left_usd  -- Goal remaining in USD

    /* Derived columns */
    , make_date(year, month, 1) as budget_month  -- First day of the budget month as a date
    , trim(
        regexp_replace(
            regexp_replace(name, '[^\pL\pN\s.&/]+', ' ', 'g'),
            '\s+',
            ' ',
            'g'
        )
    ) as category_name  -- Cleaned category name (noise stripped, whitespace collapsed)
from raw.monthly_categories
