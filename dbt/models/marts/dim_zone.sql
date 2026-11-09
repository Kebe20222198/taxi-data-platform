-- ==============================================================================
-- Dimension: dim_zone
-- Grain: 1 zone (1 row per official TLC taxi zone)
-- Kimball: Conformed Spatial Dimension
-- ==============================================================================

{{
  config(
    materialized = 'table'
  )
}}

WITH staging_zones AS (
    SELECT *
    FROM {{ ref('stg_taxi_zones') }}
),

transformed AS (
    SELECT
        zone_id,
        zone_name,
        borough,
        service_zone,
        is_airport_zone,

        -- Derived analytical groups
        CASE
            WHEN borough = 'Manhattan' THEN 'Core Urban'
            WHEN is_airport_zone THEN 'Airport Hub'
            WHEN borough IN ('Brooklyn', 'Queens', 'Bronx', 'Staten Island') THEN 'Outer Borough'
            ELSE 'Other'
        END AS zone_category

    FROM staging_zones
)

SELECT * FROM transformed
