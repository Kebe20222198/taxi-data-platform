-- ==============================================================================
-- Singular Test: assert_quarantine_rejection_rate_normal
-- Rule: The proportion of rejections in quarantine compared to total ingested trips
--       for any month must not exceed 20.0% (configured via var 'max_quarantine_rejection_rate').
-- ==============================================================================

WITH monthly_clean AS (
    SELECT
        partition_month,
        COUNT(*) AS valid_count
    FROM {{ ref('fct_trips') }}
    GROUP BY partition_month
),

monthly_quarantine AS (
    SELECT
        partition_month,
        COUNT(*) AS rejected_count
    FROM {{ ref('stg_quarantine_trips') }}
    GROUP BY partition_month
),

evaluated AS (
    SELECT
        c.partition_month,
        c.valid_count,
        COALESCE(q.rejected_count, 0) AS rejected_count,
        (COALESCE(q.rejected_count, 0) * 1.0 / (c.valid_count + COALESCE(q.rejected_count, 0))) AS rejection_rate
    FROM monthly_clean c
    LEFT JOIN monthly_quarantine q
        ON c.partition_month = q.partition_month
)

SELECT
    partition_month,
    valid_count,
    rejected_count,
    rejection_rate
FROM evaluated
WHERE rejection_rate > {{ var('max_quarantine_rejection_rate', 0.20) }}
