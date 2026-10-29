SELECT
    pickup_zone,
    pickup_borough,
    hour(pickup_at) AS pickup_hour,
    count(*) AS trip_count,
    sum(total_amount) AS net_total_amount
FROM {trips}
WHERE source_month = '2024-01'
  AND pickup_at >= TIMESTAMP_NTZ '2024-01-01 00:00:00'
  AND pickup_at < TIMESTAMP_NTZ '2024-01-08 00:00:00'
GROUP BY pickup_zone, pickup_borough, hour(pickup_at)
ORDER BY trip_count DESC, pickup_zone, pickup_borough, pickup_hour
LIMIT 100
