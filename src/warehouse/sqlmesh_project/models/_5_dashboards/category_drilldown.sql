MODEL (
  name dashboards.category_drilldown,
  kind FULL,
  grain (year, category_id),
  audits (
    not_null(columns := (year, category_id)),
    unique_combination_of_columns(columns := (year, category_id))
  ),
  description 'Tab 4 (ETH-472): per-year, per-category Needs/Wants drilldown — spend, current-year projection, and share of bucket. Passthrough of core.yearly_category_adherence; category_id is the grain key and is dropped at render time.'
);

select
    year
    , category_id
    , bucket
    , category
    , amount_spent
    , projected_current_year
    , pct_of_bucket
    , is_extrapolated
from core.yearly_category_adherence
order by year desc, bucket, pct_of_bucket desc
