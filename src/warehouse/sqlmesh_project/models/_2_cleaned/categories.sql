MODEL (
  name cleaned.categories,
  kind FULL,
  grain id,
  description 'Distinct YNAB categories — one row per category UUID, taking the most recent monthly snapshot from raw.monthly_categories. Per-month money columns live in cleaned.monthly_categories.'
);

select
    /* Primary key */
    id  -- YNAB category UUID

    /* Foreign keys */
    , category_group_id  -- Parent category group UUID

    /* Status and properties */
    , name  -- Raw category name (may contain emojis or other noisy chars)
    , hidden  -- Whether the category is hidden in YNAB
    , deleted  -- Whether the category has been deleted
    , note  -- User-entered note (nullable, from latest snapshot)

    /* Derived columns */
    , trim(
        regexp_replace(
            regexp_replace(name, '[^\pL\pN\s.&/+]+', ' ', 'g'),
            '\s+',
            ' ',
            'g'
        )
    ) as category_name  -- Cleaned category name (noise stripped, whitespace collapsed)
    , coalesce(hidden, false) as is_hidden  -- True when hidden = true (NULL coerced to false)
from raw.monthly_categories
qualify row_number() over (partition by id order by month desc, year desc) = 1
