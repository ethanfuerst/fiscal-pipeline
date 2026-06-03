AUDIT (
  name yearly_bucket_adherence_overage_consistency
);

SELECT
  year,
  bucket,
  target,
  projected,
  overage_pct
FROM @this_model
WHERE target IS NOT NULL
  AND target <> 0
  AND NOT abs(overage_pct - ((projected - target) / target)) < 0.0001;
