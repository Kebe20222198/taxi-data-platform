-- ==============================================================================
-- Quality Monitoring Model: quarantine_summary
-- Grain: 1 row per rejection reason × partition month
-- Kimball: Diagnostic Quality Mart for Quarantine Observability
-- ==============================================================================

{{
  config(
    materialized = 'view'
  )
}}

WITH base_quarantine AS (
    SELECT
        partition_month,
        reject_reason,
        duration_minutes,
        trip_distance_miles,
        total_amount,
        pickup_datetime
    FROM {{ ref('stg_quarantine_trips') }}
),

reason_aggregated AS (
    SELECT
        partition_month,
        reject_reason,

        -- Volume of rejections for this specific rule violation
        COUNT(*) AS rejected_records_count,

        -- Anomaly metrics profiling
        ROUND(CAST(AVG(duration_minutes) AS NUMERIC(10, 2)), 2) AS avg_invalid_duration_minutes,
        ROUND(CAST(AVG(trip_distance_miles) AS NUMERIC(10, 2)), 2) AS avg_invalid_distance_miles,
        ROUND(CAST(AVG(total_amount) AS NUMERIC(10, 2)), 2) AS avg_invalid_total_amount,
        MIN(pickup_datetime) AS min_pickup_datetime,
        MAX(pickup_datetime) AS max_pickup_datetime

    FROM base_quarantine
    GROUP BY
        partition_month,
        reject_reason
),

with_percentages AS (
    SELECT
        partition_month,
        reject_reason,
        rejected_records_count,

        -- Total rejections in that month across all reasons
        SUM(rejected_records_count) OVER (PARTITION BY partition_month) AS total_month_rejections,

        -- Percentage of monthly rejections represented by this cause
        ROUND(
            CAST(
                (rejected_records_count * 100.0 / NULLIF(SUM(rejected_records_count) OVER (PARTITION BY partition_month), 0))
                AS NUMERIC(6, 2)
            ),
            2
        ) AS rejection_share_pct,

        avg_invalid_duration_minutes,
        avg_invalid_distance_miles,
        avg_invalid_total_amount,
        min_pickup_datetime,
        max_pickup_datetime

    FROM reason_aggregated
)

SELECT * FROM with_percentages
ORDER BY
    partition_month DESC,
    rejected_records_count DESC
