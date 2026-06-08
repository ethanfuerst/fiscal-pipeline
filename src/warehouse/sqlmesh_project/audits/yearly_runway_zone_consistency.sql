AUDIT (
  name yearly_runway_zone_consistency
);

SELECT
  year,
  worst_runway,
  zone
FROM @this_model
WHERE zone <> CASE
    WHEN worst_runway IS NULL THEN 'Healthy'
    WHEN worst_runway >= 2.00 THEN 'Healthy'
    WHEN worst_runway >= 1.50 THEN 'Watch'
    ELSE 'Unhealthy'
  END;
