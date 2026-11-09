-- ==============================================================================
-- Singular Test: assert_valid_pickup_dropoff_dates
-- Rule: Pickup timestamp must strictly precede or equal dropoff timestamp.
-- Return: Fails if any row has pickup_datetime > dropoff_datetime.
-- ==============================================================================

SELECT
    trip_id,
    pickup_datetime,
    dropoff_datetime
FROM {{ ref('fct_trips') }}
WHERE
    pickup_datetime > dropoff_datetime
