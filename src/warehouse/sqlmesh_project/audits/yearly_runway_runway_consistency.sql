AUDIT (
  name yearly_runway_runway_consistency
);

SELECT
  year
FROM @this_model
WHERE
  (net_burn_trailing_3mo > 0 AND (runway_trailing_3mo IS NULL
     OR NOT abs(runway_trailing_3mo - round(liquid_cash / net_burn_trailing_3mo, 2)) < 0.01))
  OR (net_burn_trailing_3mo = 0 AND runway_trailing_3mo IS NOT NULL)
  OR (net_burn_trailing_12mo > 0 AND (runway_trailing_12mo IS NULL
     OR NOT abs(runway_trailing_12mo - round(liquid_cash / net_burn_trailing_12mo, 2)) < 0.01))
  OR (net_burn_trailing_12mo = 0 AND runway_trailing_12mo IS NOT NULL)
  OR (net_burn_projected > 0 AND (runway_projected IS NULL
     OR NOT abs(runway_projected - round(liquid_cash / net_burn_projected, 2)) < 0.01))
  OR (net_burn_projected = 0 AND runway_projected IS NOT NULL)
  OR (gross_burn_trailing_3mo > 0 AND (gross_runway_trailing_3mo IS NULL
     OR NOT abs(gross_runway_trailing_3mo - round(liquid_cash / gross_burn_trailing_3mo, 2)) < 0.01))
  OR (gross_burn_trailing_3mo = 0 AND gross_runway_trailing_3mo IS NOT NULL)
  OR (gross_burn_trailing_12mo > 0 AND (gross_runway_trailing_12mo IS NULL
     OR NOT abs(gross_runway_trailing_12mo - round(liquid_cash / gross_burn_trailing_12mo, 2)) < 0.01))
  OR (gross_burn_trailing_12mo = 0 AND gross_runway_trailing_12mo IS NOT NULL);
