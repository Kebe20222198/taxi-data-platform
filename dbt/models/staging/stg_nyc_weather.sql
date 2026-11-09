-- ==============================================================================
-- Staging Model: stg_nyc_weather
-- Materialization: view
-- Source: NYC Weather 2025 Reference (Bronze Layer)
-- Description: Standardizes daily weather metrics for NYC (Central Park / Open-Meteo)
-- ==============================================================================

WITH raw_weather AS (
    SELECT *
    FROM {{ source_ref_weather() }}
),

standardized AS (
    SELECT
        CAST(time AS DATE) AS date_day,
        CAST(time AS DATE) AS date,
        CAST(temperature_2m_max AS DOUBLE) AS temperature_max_c,
        CAST(temperature_2m_min AS DOUBLE) AS temperature_min_c,
        CAST(precipitation_sum AS DOUBLE) AS precipitation_mm,
        CAST(snowfall_sum AS DOUBLE) AS snowfall_cm
    FROM raw_weather
)

SELECT * FROM standardized
