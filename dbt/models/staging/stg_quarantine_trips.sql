-- ==============================================================================
-- Staging Model: stg_quarantine_trips
-- Materialization: view
-- Source: Quarantine Layer (MinIO / PostgreSQL)
-- Description: Standardizes invalid/rejected records and quality violation reasons
-- ==============================================================================

WITH raw_quarantine AS (
    SELECT *
    FROM {{ source_quarantine_yellow() }}
),

standardized AS (
    SELECT
        -- Rejection Cause Metadata
        COALESCE(reject_reason, 'unspecified_error') AS reject_reason,

        -- Primary violation categories (boolean indicators for analytics)
        CASE WHEN reject_reason LIKE '%invalid_duration%' THEN TRUE ELSE FALSE END AS has_invalid_duration,
        CASE WHEN reject_reason LIKE '%invalid_distance%' THEN TRUE ELSE FALSE END AS has_invalid_distance,
        CASE WHEN reject_reason LIKE '%invalid_total_amount%' THEN TRUE ELSE FALSE END AS has_invalid_amount,
        CASE WHEN reject_reason LIKE '%pickup_outside_month%' THEN TRUE ELSE FALSE END AS has_timestamp_out_of_bounds,
        CASE WHEN reject_reason LIKE '%invalid_zone_id%' THEN TRUE ELSE FALSE END AS has_invalid_zone,

        -- Trip attributes
        CAST(vendor_id AS INTEGER) AS vendor_id,
        CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime,
        CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
        CAST(passenger_count AS INTEGER) AS passenger_count,
        ROUND(CAST(trip_distance AS NUMERIC(10, 2)), 2) AS trip_distance_miles,
        CAST(pu_location_id AS INTEGER) AS pu_location_id,
        CAST(do_location_id AS INTEGER) AS do_location_id,
        CAST(payment_type AS INTEGER) AS payment_type_id,
        ROUND(CAST(fare_amount AS NUMERIC(10, 2)), 2) AS fare_amount,
        ROUND(CAST(total_amount AS NUMERIC(10, 2)), 2) AS total_amount,

        -- Approximated duration in minutes
        CASE
            WHEN tpep_pickup_datetime IS NOT NULL AND tpep_dropoff_datetime IS NOT NULL THEN
                ROUND(
                    CAST(
                        (EXTRACT(EPOCH FROM CAST(tpep_dropoff_datetime AS TIMESTAMP))
                         - EXTRACT(EPOCH FROM CAST(tpep_pickup_datetime AS TIMESTAMP))) / 60.0
                        AS NUMERIC(10, 2)
                    ),
                    2
                )
            ELSE NULL
        END AS duration_minutes,

        -- Partitioning Columns
        COALESCE(
            CAST(year AS INTEGER),
            CAST(EXTRACT(YEAR FROM CAST(tpep_pickup_datetime AS TIMESTAMP)) AS INTEGER)
        ) AS partition_year,
        COALESCE(
            CAST(month AS INTEGER),
            CAST(EXTRACT(MONTH FROM CAST(tpep_pickup_datetime AS TIMESTAMP)) AS INTEGER)
        ) AS partition_month_num,

        -- Standardized YYYY-MM string
        CASE
            WHEN CAST(month AS INTEGER) < 10 THEN
                CONCAT(CAST(year AS VARCHAR), '-0', CAST(month AS VARCHAR))
            ELSE
                CONCAT(CAST(year AS VARCHAR), '-', CAST(month AS VARCHAR))
        END AS partition_month

    FROM raw_quarantine
)

SELECT * FROM standardized
