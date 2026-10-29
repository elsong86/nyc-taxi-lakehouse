SELECT
    pickup_borough,
    pickup_hour,
    sum(trip_count) AS trip_count,
    sum(net_total_amount) AS net_total_amount
FROM {hourly_zone_demand}
GROUP BY pickup_borough, pickup_hour
ORDER BY pickup_borough, pickup_hour
