-- ==============================================================================
-- Staging Model: stg_silver_trips
-- Materialization: view
-- Source: Silver Delta Layer (MinIO / PostgreSQL)
-- Description: Standardizes column types, snake_case aliases, and partitions
-- ==============================================================================

WITH raw_silver AS (
    SELECT *
    FROM {{ source_silver_yellow() }}
),

standardized AS (
    SELECT
        -- Primary Key (SHA-256 hash generated in Spark Silver layer)
        COALESCE(
            trip_hash,
            MD5(
                CONCAT_WS(
                    '-',
                    CAST(vendor_id AS VARCHAR),
                    CAST(tpep_pickup_datetime AS VARCHAR),
                    CAST(pu_location_id AS VARCHAR),
                    CAST(do_location_id AS VARCHAR),
                    CAST(fare_amount AS VARCHAR)
                )
            )
        ) AS trip_id,

        -- Vendor
        CAST(vendor_id AS INTEGER) AS vendor_id,

        -- Timestamps
        CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime,
        CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,

        -- Passenger & Trip Attributes
        COALESCE(CAST(passenger_count AS INTEGER), 1) AS passenger_count,
        ROUND(CAST(trip_distance AS NUMERIC(10, 2)), 2) AS trip_distance_miles,
        CAST(ratecode_id AS INTEGER) AS ratecode_id,
        COALESCE(store_and_fwd_flag, 'N') AS store_and_fwd_flag,

        -- Zone Location Foreign Keys
        CAST(pu_location_id AS INTEGER) AS pu_location_id,
        CAST(do_location_id AS INTEGER) AS do_location_id,

        -- Payment Foreign Key (defaulting to 5 = Unknown if null)
        COALESCE(CAST(payment_type AS INTEGER), 5) AS payment_type_id,

        -- Financial Amounts (Cast to Numeric currency precision)
        ROUND(CAST(fare_amount AS NUMERIC(10, 2)), 2) AS fare_amount,
        ROUND(COALESCE(CAST(extra AS NUMERIC(10, 2)), 0.00), 2) AS extra_amount,
        ROUND(COALESCE(CAST(mta_tax AS NUMERIC(10, 2)), 0.00), 2) AS mta_tax_amount,
        ROUND(COALESCE(CAST(tip_amount AS NUMERIC(10, 2)), 0.00), 2) AS tip_amount,
        ROUND(COALESCE(CAST(tolls_amount AS NUMERIC(10, 2)), 0.00), 2) AS tolls_amount,
        ROUND(COALESCE(CAST(improvement_surcharge AS NUMERIC(10, 2)), 0.00), 2) AS improvement_surcharge,
        ROUND(COALESCE(CAST(congestion_surcharge AS NUMERIC(10, 2)), 0.00), 2) AS congestion_surcharge,
        ROUND(COALESCE(CAST(airport_fee AS NUMERIC(10, 2)), 0.00), 2) AS airport_fee,
        ROUND(CAST(total_amount AS NUMERIC(10, 2)), 2) AS total_amount,

        -- Derived Metrics & Flags
        ROUND(CAST(duration_minutes AS NUMERIC(10, 2)), 2) AS duration_minutes,
        ROUND(CAST(avg_speed_mph AS NUMERIC(10, 2)), 2) AS avg_speed_mph,
        CASE
            WHEN is_airport_trip IS TRUE
                 OR CAST(is_airport_trip AS VARCHAR) IN ('Y', 'true', '1')
                 OR pu_location_id IN (1, 132, 138)
                 OR do_location_id IN (1, 132, 138)
            THEN TRUE
            ELSE FALSE
        END AS is_airport_trip,

        -- Partitioning Columns
        CAST(year AS INTEGER) AS partition_year,
        CAST(month AS INTEGER) AS partition_month_num,
        -- Standardized YYYY-MM string
        CASE
            WHEN CAST(month AS INTEGER) < 10 THEN
                CONCAT(CAST(year AS VARCHAR), '-0', CAST(month AS VARCHAR))
            ELSE
                CONCAT(CAST(year AS VARCHAR), '-', CAST(month AS VARCHAR))
        END AS partition_month

    FROM raw_silver
)

SELECT * FROM standardized
