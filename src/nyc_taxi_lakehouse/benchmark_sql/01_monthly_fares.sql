SELECT
    date_format(pickup_at, 'yyyy-MM') AS pickup_month,
    count(*) AS trip_count,
    sum(total_amount) AS net_total_amount,
    round(avg(trip_distance_miles), 2) AS average_distance_miles
FROM {trips}
GROUP BY date_format(pickup_at, 'yyyy-MM')
ORDER BY pickup_month
