MODEL (
  name cleaned.category_groups,
  kind FULL,
  grain id,
  description 'Cleaned YNAB category groups. Splits the " - " separated name into category_group_name_mapping and subcategory_group_name.'
);

select
    /* Primary key */
    id  -- YNAB category group UUID

    /* Status and properties */
    , name  -- Category group display name (uses " - " separator for top-level group and subgroup)
    , hidden  -- Whether the category group is hidden in YNAB
    , deleted  -- Whether the category group has been deleted

    /* Derived columns */
    , case
        when name = 'Internal Master Category'
            then 'Income'
        else split(name, ' - ')[1]
    end as category_group_name_mapping  -- Top-level group from name (or 'Income' for the internal master category)
    , split(name, ' - ')[2] as subcategory_group_name  -- Subgroup name after the " - " separator (nullable)
from raw.category_groups
