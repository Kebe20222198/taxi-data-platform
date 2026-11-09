-- ==============================================================================
-- Singular Test: assert_fct_trips_positive_metrics
-- Rule: Every valid trip in fct_trips must have strictly positive metrics:
--       distance > 0, duration > 0, total_amount > 0
-- Return: Fails if any row violates these constraints.
-- ==============================================================================

SELECT
    trip_id,
    trip_distance_miles,
    duration_minutes,
    total_amount
FROM {{ ref('fct_trips') }}
WHERE
    trip_distance_miles <= 0
    OR duration_minutes <= 0
    OR total_amount <= 0
