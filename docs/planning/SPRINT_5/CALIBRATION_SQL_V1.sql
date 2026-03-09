-- Sprint 5 calibration support queries (v1)
--
-- Usage example (PostgreSQL):
--   psql "${DATABASE_URL/postgresql+psycopg/postgresql}" -v ON_ERROR_STOP=1 -f docs/planning/SPRINT_5/CALIBRATION_SQL_V1.sql
--
-- Update the target versions in each `target` CTE to match the run you are calibrating.

-- -----------------------------------------------------------------------------
-- 1) Run inventory and score coverage by run
-- -----------------------------------------------------------------------------
WITH target AS (
    SELECT
        'scoring_rules_v1' AS ruleset_version,
        'feature_v1' AS feature_version
),
scoped_runs AS (
    SELECT
        sr.id AS run_id,
        sr.started_at,
        sr.completed_at,
        sr.status,
        COALESCE((sr.output_summary_json ->> 'features_considered')::INTEGER, 0) AS features_considered,
        COALESCE((sr.output_summary_json ->> 'scores_inserted')::INTEGER, 0) AS scores_inserted,
        COALESCE((sr.output_summary_json ->> 'flags_inserted')::INTEGER, 0) AS flags_inserted
    FROM scoring_runs AS sr
    JOIN target AS t
        ON sr.version_tag = t.ruleset_version
    WHERE sr.run_type = 'score_fraud'
),
scoped_scores AS (
    SELECT
        fs.run_id,
        COUNT(*) AS score_rows
    FROM fraud_scores AS fs
    JOIN target AS t
        ON fs.ruleset_version = t.ruleset_version
       AND fs.feature_version = t.feature_version
    GROUP BY fs.run_id
)
SELECT
    r.run_id,
    r.status,
    r.started_at,
    r.completed_at,
    r.features_considered,
    r.scores_inserted,
    r.flags_inserted,
    COALESCE(s.score_rows, 0) AS persisted_score_rows
FROM scoped_runs AS r
LEFT JOIN scoped_scores AS s
    ON s.run_id = r.run_id
ORDER BY r.run_id DESC;

-- -----------------------------------------------------------------------------
-- 2) Score distribution buckets for triage workload planning
-- -----------------------------------------------------------------------------
WITH target AS (
    SELECT
        'scoring_rules_v1' AS ruleset_version,
        'feature_v1' AS feature_version
),
scoped_scores AS (
    SELECT fs.score_value
    FROM fraud_scores AS fs
    JOIN target AS t
        ON fs.ruleset_version = t.ruleset_version
       AND fs.feature_version = t.feature_version
)
SELECT
    bucket_label,
    COUNT(*) AS parcel_count,
    MIN(score_value) AS min_score,
    MAX(score_value) AS max_score
FROM (
    SELECT
        score_value,
        CASE
            WHEN score_value >= 90 THEN '90-100'
            WHEN score_value >= 80 THEN '80-89.99'
            WHEN score_value >= 70 THEN '70-79.99'
            WHEN score_value >= 60 THEN '60-69.99'
            WHEN score_value >= 50 THEN '50-59.99'
            WHEN score_value >= 40 THEN '40-49.99'
            WHEN score_value >= 30 THEN '30-39.99'
            WHEN score_value >= 20 THEN '20-29.99'
            WHEN score_value >= 10 THEN '10-19.99'
            ELSE '0-9.99'
        END AS bucket_label,
        CASE
            WHEN score_value >= 90 THEN 1
            WHEN score_value >= 80 THEN 2
            WHEN score_value >= 70 THEN 3
            WHEN score_value >= 60 THEN 4
            WHEN score_value >= 50 THEN 5
            WHEN score_value >= 40 THEN 6
            WHEN score_value >= 30 THEN 7
            WHEN score_value >= 20 THEN 8
            WHEN score_value >= 10 THEN 9
            ELSE 10
        END AS bucket_order
    FROM scoped_scores
) AS bucketed
GROUP BY bucket_label, bucket_order
ORDER BY bucket_order;

-- -----------------------------------------------------------------------------
-- 3) Threshold sweep: compare review queue size under alternative cutoffs
-- -----------------------------------------------------------------------------
WITH target AS (
    SELECT
        'scoring_rules_v1' AS ruleset_version,
        'feature_v1' AS feature_version
),
scored AS (
    SELECT fs.score_value
    FROM fraud_scores AS fs
    JOIN target AS t
        ON fs.ruleset_version = t.ruleset_version
       AND fs.feature_version = t.feature_version
),
threshold_pairs AS (
    SELECT 80.0 AS high_min, 50.0 AS medium_min
    UNION ALL SELECT 80.0, 45.0
    UNION ALL SELECT 80.0, 40.0
    UNION ALL SELECT 75.0, 45.0
    UNION ALL SELECT 75.0, 40.0
    UNION ALL SELECT 75.0, 35.0
    UNION ALL SELECT 70.0, 40.0
    UNION ALL SELECT 70.0, 35.0
    UNION ALL SELECT 70.0, 30.0
    UNION ALL SELECT 65.0, 35.0
    UNION ALL SELECT 65.0, 30.0
    UNION ALL SELECT 60.0, 30.0
),
evaluated AS (
    SELECT
        tp.high_min,
        tp.medium_min,
        SUM(CASE WHEN s.score_value >= tp.high_min THEN 1 ELSE 0 END) AS high_count,
        SUM(
            CASE
                WHEN s.score_value >= tp.medium_min AND s.score_value < tp.high_min THEN 1
                ELSE 0
            END
        ) AS medium_count,
        SUM(CASE WHEN s.score_value < tp.medium_min THEN 1 ELSE 0 END) AS low_count,
        COUNT(*) AS total_scored
    FROM threshold_pairs AS tp
    CROSS JOIN scored AS s
    GROUP BY tp.high_min, tp.medium_min
)
SELECT
    high_min,
    medium_min,
    high_count,
    medium_count,
    low_count,
    (high_count + medium_count) AS review_queue_count,
    ROUND(
        CASE
            WHEN total_scored = 0 THEN 0.0
            ELSE 100.0 * (high_count + medium_count) / total_scored
        END,
        2
    ) AS review_queue_pct
FROM evaluated
ORDER BY review_queue_count ASC, high_min DESC, medium_min DESC;

-- -----------------------------------------------------------------------------
-- 4) Reason-code mix under a selected threshold policy
-- -----------------------------------------------------------------------------
WITH target AS (
    SELECT
        'scoring_rules_v1' AS ruleset_version,
        'feature_v1' AS feature_version,
        70.0 AS high_min,
        40.0 AS medium_min
),
scoped_scores AS (
    SELECT
        fs.id AS score_id,
        fs.parcel_id,
        fs.year,
        fs.score_value
    FROM fraud_scores AS fs
    JOIN target AS t
        ON fs.ruleset_version = t.ruleset_version
       AND fs.feature_version = t.feature_version
),
labeled_scores AS (
    SELECT
        ss.score_id,
        ss.parcel_id,
        ss.year,
        ss.score_value,
        CASE
            WHEN ss.score_value >= t.high_min THEN 'high'
            WHEN ss.score_value >= t.medium_min THEN 'medium'
            ELSE 'low'
        END AS calibrated_band
    FROM scoped_scores AS ss
    CROSS JOIN target AS t
)
SELECT
    ls.calibrated_band,
    ff.reason_code,
    COUNT(*) AS flag_count,
    COUNT(DISTINCT ls.score_id) AS parcel_count
FROM labeled_scores AS ls
JOIN fraud_flags AS ff
    ON ff.score_id = ls.score_id
GROUP BY ls.calibrated_band, ff.reason_code
ORDER BY ls.calibrated_band, flag_count DESC, ff.reason_code ASC;

-- -----------------------------------------------------------------------------
-- 5) Borderline parcel review set around current thresholds
-- -----------------------------------------------------------------------------
WITH target AS (
    SELECT
        'scoring_rules_v1' AS ruleset_version,
        'feature_v1' AS feature_version,
        70.0 AS high_min,
        40.0 AS medium_min,
        5.0 AS window_size
),
scoped_scores AS (
    SELECT
        fs.parcel_id,
        fs.year,
        fs.score_value,
        fs.reason_code_count,
        fs.risk_band
    FROM fraud_scores AS fs
    JOIN target AS t
        ON fs.ruleset_version = t.ruleset_version
       AND fs.feature_version = t.feature_version
)
SELECT
    ss.parcel_id,
    ss.year,
    ss.score_value,
    ss.reason_code_count,
    ss.risk_band,
    CASE
        WHEN ABS(ss.score_value - t.high_min) <= t.window_size THEN 'high_threshold_window'
        WHEN ABS(ss.score_value - t.medium_min) <= t.window_size THEN 'medium_threshold_window'
        ELSE 'outside_window'
    END AS threshold_window
FROM scoped_scores AS ss
CROSS JOIN target AS t
WHERE
    ABS(ss.score_value - t.high_min) <= t.window_size
    OR ABS(ss.score_value - t.medium_min) <= t.window_size
ORDER BY
    CASE
        WHEN ABS(ss.score_value - t.high_min) <= t.window_size THEN ABS(ss.score_value - t.high_min)
        ELSE ABS(ss.score_value - t.medium_min)
    END ASC,
    ss.score_value DESC,
    ss.parcel_id ASC,
    ss.year ASC
LIMIT 250;

-- -----------------------------------------------------------------------------
-- 6) Area pattern rollup (TRS code) for neighborhood-level review planning
-- -----------------------------------------------------------------------------
WITH target AS (
    SELECT
        'scoring_rules_v1' AS ruleset_version,
        'feature_v1' AS feature_version,
        70.0 AS high_min,
        40.0 AS medium_min
),
scoped_scores AS (
    SELECT
        fs.parcel_id,
        fs.year,
        fs.score_value
    FROM fraud_scores AS fs
    JOIN target AS t
        ON fs.ruleset_version = t.ruleset_version
       AND fs.feature_version = t.feature_version
),
area_rollup AS (
    SELECT
        COALESCE(p.trs_code, '(missing)') AS trs_code,
        COUNT(*) AS parcel_count,
        AVG(ss.score_value) AS avg_score,
        SUM(CASE WHEN ss.score_value >= t.high_min THEN 1 ELSE 0 END) AS high_count,
        SUM(
            CASE
                WHEN ss.score_value >= t.medium_min AND ss.score_value < t.high_min THEN 1
                ELSE 0
            END
        ) AS medium_count
    FROM scoped_scores AS ss
    LEFT JOIN parcels AS p
        ON p.id = ss.parcel_id
    CROSS JOIN target AS t
    GROUP BY COALESCE(p.trs_code, '(missing)')
)
SELECT
    trs_code,
    parcel_count,
    ROUND(avg_score, 2) AS avg_score,
    high_count,
    medium_count,
    ROUND(
        CASE
            WHEN parcel_count = 0 THEN 0.0
            ELSE 100.0 * (high_count + medium_count) / parcel_count
        END,
        2
    ) AS review_queue_pct
FROM area_rollup
WHERE parcel_count >= 5
ORDER BY review_queue_pct DESC, avg_score DESC, trs_code ASC;
