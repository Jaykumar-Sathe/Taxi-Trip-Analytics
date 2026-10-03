-- name: vendor_comparison
SELECT vendor_name,
       COUNT(*)                              AS trips,
       ROUND(SUM(total_amount), 2)           AS revenue,
       ROUND(AVG(fare_amount), 2)            AS avg_fare,
       ROUND(AVG(trip_distance), 2)          AS avg_distance,
       ROUND(AVG(trip_duration_minutes), 2)  AS avg_duration_min,
       ROUND(AVG(tip_amount), 2)             AS avg_tip
FROM taxi_trips
GROUP BY vendor_name
ORDER BY trips DESC;

-- name: monthly_vendor_rank
-- RANK() partitioned by month
SELECT pickup_year, pickup_month, vendor_name,
       ROUND(SUM(total_amount), 2) AS revenue,
       RANK() OVER (PARTITION BY pickup_year, pickup_month ORDER BY SUM(total_amount) DESC) AS revenue_rank
FROM taxi_trips
GROUP BY pickup_year, pickup_month, vendor_name
ORDER BY pickup_year, pickup_month, revenue_rank;
