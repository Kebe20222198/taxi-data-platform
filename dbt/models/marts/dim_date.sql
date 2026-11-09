-- ==============================================================================
-- Dimension: dim_date
-- Grain: 1 day (1 row per calendar day)
-- Kimball: Conformed Date Dimension with US federal holidays & weather enrichment
-- ==============================================================================

{{
  config(
    materialized = 'table'
  )
}}

WITH date_spine AS (
    {% if target.type == 'duckdb' %}
    SELECT CAST(range AS DATE) AS date_day
    FROM range(
        DATE '{{ var("dim_date_start", "2025-01-01") }}',
        DATE '{{ var("dim_date_end", "2025-12-31") }}' + INTERVAL 1 DAY,
        INTERVAL 1 DAY
    )
    {% else %}
    SELECT CAST(d AS DATE) AS date_day
    FROM generate_series(
        '{{ var("dim_date_start", "2025-01-01") }}'::date,
        '{{ var("dim_date_end", "2025-12-31") }}'::date,
        '1 day'::interval
    ) AS d
    {% endif %}
),

holidays AS (
    SELECT
        holiday_date,
        holiday_name
    FROM {{ ref('stg_us_holidays') }}
),

weather AS (
    SELECT
        date_day,
        temperature_max_c,
        temperature_min_c,
        precipitation_mm,
        snowfall_cm
    FROM {{ ref('stg_nyc_weather') }}
),

transformed AS (
    SELECT
        -- Surrogate Key (Smart integer key YYYYMMDD)
        CAST(
            EXTRACT(YEAR FROM s.date_day) * 10000
            + EXTRACT(MONTH FROM s.date_day) * 100
            + EXTRACT(DAY FROM s.date_day)
            AS INTEGER
        ) AS date_id,

        s.date_day,
        CAST(EXTRACT(DAY FROM s.date_day) AS INTEGER) AS day_of_month,
        CAST(EXTRACT(MONTH FROM s.date_day) AS INTEGER) AS month_number,

        CASE CAST(EXTRACT(MONTH FROM s.date_day) AS INTEGER)
            WHEN 1 THEN 'January'
            WHEN 2 THEN 'February'
            WHEN 3 THEN 'March'
            WHEN 4 THEN 'April'
            WHEN 5 THEN 'May'
            WHEN 6 THEN 'June'
            WHEN 7 THEN 'July'
            WHEN 8 THEN 'August'
            WHEN 9 THEN 'September'
            WHEN 10 THEN 'October'
            WHEN 11 THEN 'November'
            WHEN 12 THEN 'December'
        END AS month_name,

        CAST(EXTRACT(YEAR FROM s.date_day) AS INTEGER) AS year_number,
        CAST(EXTRACT(QUARTER FROM s.date_day) AS INTEGER) AS quarter_number,

        -- 0 = Sunday, 1 = Monday, ..., 6 = Saturday
        CAST(EXTRACT(DOW FROM s.date_day) AS INTEGER) AS day_of_week_number,

        CASE CAST(EXTRACT(DOW FROM s.date_day) AS INTEGER)
            WHEN 0 THEN 'Sunday'
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
        END AS day_of_week_name,

        -- Weekend Flag
        CASE
            WHEN CAST(EXTRACT(DOW FROM s.date_day) AS INTEGER) IN (0, 6) THEN TRUE
            ELSE FALSE
        END AS is_weekend,

        -- US Federal Holiday Flag
        CASE
            WHEN h.holiday_name IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS is_us_holiday,

        h.holiday_name,

        -- Weather Metrics (NYC Central Park / Open-Meteo)
        w.temperature_max_c,
        w.temperature_min_c,
        w.precipitation_mm,
        w.snowfall_cm

    FROM date_spine s
    LEFT JOIN holidays h
        ON s.date_day = h.holiday_date
    LEFT JOIN weather w
        ON s.date_day = w.date_day
)

SELECT * FROM transformed
