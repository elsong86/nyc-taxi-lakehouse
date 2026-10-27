USE CATALOG nyc_taxi_dev;

SELECT
  pickup_zone,
  pickup_borough,
  pickup_hour,
  sum(trip_count) AS trips
FROM gold.hourly_zone_demand
GROUP BY pickup_zone, pickup_borough, pickup_hour
ORDER BY trips DESC
LIMIT 100;

SELECT *
FROM gold.monthly_fares
ORDER BY pickup_month;

SELECT *
FROM gold.airport_activity
ORDER BY trip_count DESC
LIMIT 100;

SELECT *
FROM gold.trip_patterns
ORDER BY pickup_month, pickup_hour;
