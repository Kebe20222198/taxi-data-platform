-- ==============================================================================
-- Staging Model: stg_taxi_zones
-- Materialization: view
-- Source: Reference Taxi Zone Lookup (Bronze Layer)
-- Description: Standardizes TLC Taxi Zones and spatial classifications
-- ==============================================================================

WITH raw_zones AS (
    SELECT *
    FROM {{ source_ref_taxi_zones() }}
),

standardized AS (
    SELECT
        CAST(LocationID AS INTEGER) AS zone_id,
        CASE
            WHEN TRIM(Borough) IN ('', 'N/A') OR Borough IS NULL THEN 'Unknown'
            ELSE TRIM(Borough)
        END AS borough,
        COALESCE(
            NULLIF(TRIM(Zone), ''),
            'Unknown Zone'
        ) AS zone_name,
        COALESCE(
            NULLIF(TRIM(service_zone), ''),
            'N/A'
        ) AS service_zone,
        -- Airport Indicator
        CASE
            WHEN CAST(LocationID AS INTEGER) IN (1, 132, 138)
                 OR LOWER(Zone) LIKE '%airport%'
            THEN TRUE
            ELSE FALSE
        END AS is_airport_zone

    FROM raw_zones
)

SELECT * FROM standardized
