-- ==============================================================================
-- Aggregate Mart: agg_zone_hourly
-- Grain: 1 row per pickup zone × calendar date × hour of day
-- Kimball: Pre-aggregated Summary Table for Dashboard & BI acceleration
-- ==============================================================================

{{
  config(
    materialized = 'table'
  )
}}

WITH base_trips AS (
    SELECT
        pickup_zone_id,
        CAST(pickup_datetime AS DATE) AS pickup_date,
        pickup_hour,
        passenger_count,
        trip_distance_miles,
        duration_minutes,
        avg_speed_mph,
        fare_amount,
        tip_amount,
        total_amount,
        is_airport_trip
    FROM {{ ref('fct_trips') }}
),

aggregated AS (
    SELECT
        pickup_zone_id,
        pickup_date,
        pickup_hour,

        -- Volume Metrics
        COUNT(*) AS trip_count,
        SUM(passenger_count) AS total_passengers,
        SUM(CASE WHEN is_airport_trip THEN 1 ELSE 0 END) AS airport_trip_count,

        -- Financial Totals ($ USD)
        ROUND(CAST(SUM(total_amount) AS NUMERIC(12, 2)), 2) AS total_revenue,
        ROUND(CAST(SUM(fare_amount) AS NUMERIC(12, 2)), 2) AS total_fare,
        ROUND(CAST(SUM(tip_amount) AS NUMERIC(12, 2)), 2) AS total_tips,

        -- Averages & Performance Indicators
        ROUND(CAST(AVG(tip_amount) AS NUMERIC(10, 2)), 2) AS avg_tip_amount,
        ROUND(CAST(AVG(fare_amount) AS NUMERIC(10, 2)), 2) AS avg_fare_amount,
        ROUND(CAST(AVG(trip_distance_miles) AS NUMERIC(10, 2)), 2) AS avg_trip_distance_miles,
        ROUND(CAST(AVG(duration_minutes) AS NUMERIC(10, 2)), 2) AS avg_duration_minutes,
        ROUND(CAST(AVG(avg_speed_mph) AS NUMERIC(10, 2)), 2) AS avg_speed_mph

    FROM base_trips
    GROUP BY
        pickup_zone_id,
        pickup_date,
        pickup_hour
)

SELECT * FROM aggregated
