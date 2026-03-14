-- risk_band_outcomes
WITH reviewed AS (
  SELECT cr.score_id, cr.disposition, fs.risk_band
  FROM case_reviews cr
  JOIN fraud_scores fs ON fs.id = cr.score_id
  WHERE cr.feature_version = 'feature_v1'
    AND cr.ruleset_version = 'scoring_rules_v1'
    AND fs.feature_version = cr.feature_version
    AND fs.ruleset_version = cr.ruleset_version
    AND cr.status IN ('resolved', 'closed')
    AND cr.disposition IS NOT NULL
)
SELECT
  risk_band,
  COUNT(*) AS reviewed_case_count,
  SUM(CASE WHEN disposition = 'false_positive' THEN 1 ELSE 0 END) AS false_positive_count,
  CASE WHEN COUNT(*) = 0 THEN 0
       ELSE ROUND(1.0 * SUM(CASE WHEN disposition = 'false_positive' THEN 1 ELSE 0 END) / COUNT(*), 4)
  END AS false_positive_rate
FROM reviewed
GROUP BY risk_band
ORDER BY CASE risk_band WHEN 'high' THEN 0 WHEN 'medium' THEN 1 WHEN 'low' THEN 2 ELSE 99 END, risk_band;

-- reason_code_outcomes
WITH reviewed AS (
  SELECT cr.score_id, cr.disposition, fs.risk_band
  FROM case_reviews cr
  JOIN fraud_scores fs ON fs.id = cr.score_id
  WHERE cr.feature_version = 'feature_v1'
    AND cr.ruleset_version = 'scoring_rules_v1'
    AND fs.feature_version = cr.feature_version
    AND fs.ruleset_version = cr.ruleset_version
    AND cr.status IN ('resolved', 'closed')
    AND cr.disposition IS NOT NULL
)
, reason_rows AS (
  SELECT r.disposition, ff.reason_code
  FROM reviewed r
  JOIN fraud_flags ff ON ff.score_id = r.score_id
)
SELECT
  reason_code,
  COUNT(*) AS reviewed_case_count,
  SUM(CASE WHEN disposition = 'false_positive' THEN 1 ELSE 0 END) AS false_positive_count,
  CASE WHEN COUNT(*) = 0 THEN 0
       ELSE ROUND(1.0 * SUM(CASE WHEN disposition = 'false_positive' THEN 1 ELSE 0 END) / COUNT(*), 4)
  END AS false_positive_rate
FROM reason_rows
GROUP BY reason_code
ORDER BY reviewed_case_count DESC, reason_code ASC;

-- reason_code_risk_slices
WITH reviewed AS (
  SELECT cr.score_id, cr.disposition, fs.risk_band
  FROM case_reviews cr
  JOIN fraud_scores fs ON fs.id = cr.score_id
  WHERE cr.feature_version = 'feature_v1'
    AND cr.ruleset_version = 'scoring_rules_v1'
    AND fs.feature_version = cr.feature_version
    AND fs.ruleset_version = cr.ruleset_version
    AND cr.status IN ('resolved', 'closed')
    AND cr.disposition IS NOT NULL
)
, reason_rows AS (
  SELECT r.disposition, r.risk_band, ff.reason_code
  FROM reviewed r
  JOIN fraud_flags ff ON ff.score_id = r.score_id
)
SELECT
  reason_code,
  risk_band,
  COUNT(*) AS reviewed_case_count,
  SUM(CASE WHEN disposition = 'false_positive' THEN 1 ELSE 0 END) AS false_positive_count,
  CASE WHEN COUNT(*) = 0 THEN 0
       ELSE ROUND(1.0 * SUM(CASE WHEN disposition = 'false_positive' THEN 1 ELSE 0 END) / COUNT(*), 4)
  END AS false_positive_rate
FROM reason_rows
GROUP BY reason_code, risk_band
ORDER BY reason_code ASC, CASE risk_band WHEN 'high' THEN 0 WHEN 'medium' THEN 1 WHEN 'low' THEN 2 ELSE 99 END, risk_band ASC;
