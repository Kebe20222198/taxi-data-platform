-- ==============================================================================
-- Staging Model: stg_us_holidays
-- Materialization: view
-- Source: US Public Holidays Reference (Bronze Layer)
-- Description: Standardizes official US calendar holidays
-- ==============================================================================

WITH raw_holidays AS (
    SELECT *
    FROM {{ source_ref_us_holidays() }}
),

standardized AS (
    SELECT
        CAST(date AS DATE) AS holiday_date,
        TRIM(holiday_name) AS holiday_name,
        CAST(year AS INTEGER) AS holiday_year,
        CAST(month AS INTEGER) AS holiday_month,
        CAST(day AS INTEGER) AS holiday_day,
        TRIM(day_of_week) AS day_of_week_name,
        TRUE AS is_federal_holiday

    FROM raw_holidays
)

SELECT * FROM standardized
