SELECT
    pickup_location_id,
    dropoff_location_id,
    count(*) AS trip_count,
    sum(total_amount) AS net_total_amount,
    round(avg(duration_minutes), 2) AS average_duration_minutes
FROM {trips}
WHERE airport_trip
GROUP BY pickup_location_id, dropoff_location_id
ORDER BY trip_count DESC, pickup_location_id, dropoff_location_id
LIMIT 100
