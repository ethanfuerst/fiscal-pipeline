AUDIT (
  name yearly_runway_net_burn_floor
);

SELECT
  year,
  net_burn_trailing_3mo,
  net_burn_trailing_12mo,
  net_burn_projected,
  gross_burn_trailing_3mo,
  gross_burn_trailing_12mo
FROM @this_model
WHERE net_burn_trailing_3mo < 0
   OR net_burn_trailing_12mo < 0
   OR net_burn_projected < 0
   OR gross_burn_trailing_3mo < 0
   OR gross_burn_trailing_12mo < 0;
