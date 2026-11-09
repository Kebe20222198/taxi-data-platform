-- ==============================================================================
-- Fact Table: fct_trips
-- Grain: 1 taxi trip (1 row per trip)
-- Kimball: Core Transaction Fact Table
-- Materialization: Incremental with delete+insert strategy for monthly idempotence
-- ==============================================================================

{{
  config(
    materialized = 'incremental',
    unique_key = 'trip_id',
    incremental_strategy = 'delete+insert',
    on_schema_change = 'sync_all_columns'
  )
}}

WITH staging_trips AS (
    SELECT *
    FROM {{ ref('stg_silver_trips') }}

    {% if is_incremental() %}
      -- Idempotent monthly slice processing when triggered via Airflow or CLI
      {% if var('month', none) is not none %}
        WHERE partition_month = '{{ var("month") }}'
      {% else %}
        -- Fallback for continuous processing: reload recent data window
        WHERE pickup_datetime >= (SELECT COALESCE(MAX(pickup_datetime) - INTERVAL '3 day', '2023-01-01'::timestamp) FROM {{ this }})
      {% endif %}
    {% endif %}
),

transformed AS (
    SELECT
        -- Primary Key
        trip_id,

        -- ── Foreign Keys to Conformed Dimensions ─────────────────────────────
        -- Date FK (YYYYMMDD integer matching dim_date.date_id)
        CAST(
            EXTRACT(YEAR FROM pickup_datetime) * 10000
            + EXTRACT(MONTH FROM pickup_datetime) * 100
            + EXTRACT(DAY FROM pickup_datetime)
            AS INTEGER
        ) AS pickup_date_id,

        CAST(
            EXTRACT(YEAR FROM dropoff_datetime) * 10000
            + EXTRACT(MONTH FROM dropoff_datetime) * 100
            + EXTRACT(DAY FROM dropoff_datetime)
            AS INTEGER
        ) AS dropoff_date_id,

        -- Time / Hour Keys
        CAST(EXTRACT(HOUR FROM pickup_datetime) AS INTEGER) AS pickup_hour,
        CAST(EXTRACT(HOUR FROM dropoff_datetime) AS INTEGER) AS dropoff_hour,

        -- Spatial Zone FKs
        pu_location_id AS pickup_zone_id,
        do_location_id AS dropoff_zone_id,

        -- Payment FK
        payment_type_id,

        -- Vendor & Ratecode
        vendor_id,
        ratecode_id,

        -- ── Degenerate Dimensions & Timestamps ────────────────────────────────
        pickup_datetime,
        dropoff_datetime,
        store_and_fwd_flag,
        is_airport_trip,

        -- ── Additive Numerical Metrics ───────────────────────────────────────
        passenger_count,
        trip_distance_miles,
        duration_minutes,
        avg_speed_mph,

        -- ── Detailed Financial Metrics ($ USD) ───────────────────────────────
        fare_amount,
        extra_amount,
        mta_tax_amount,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        congestion_surcharge,
        airport_fee,
        total_amount,

        -- Calculated Business KPIs
        CASE
            WHEN fare_amount > 0 THEN
                ROUND(CAST((tip_amount * 100.0 / fare_amount) AS NUMERIC(10, 2)), 2)
            ELSE 0.00
        END AS tip_percentage,

        -- ── Partitioning & Lineage Metadata ──────────────────────────────────
        partition_year,
        partition_month_num,
        partition_month

    FROM staging_trips
)

SELECT * FROM transformed
