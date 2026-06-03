AUDIT (
  name yearly_category_adherence_pct_of_bucket_sum
);

SELECT
  year,
  bucket,
  sum(pct_of_bucket) AS total_pct
FROM @this_model
WHERE pct_of_bucket IS NOT NULL
GROUP BY year, bucket
HAVING NOT abs(sum(pct_of_bucket) - 1.0) < 0.01;
