-- name: busiest_hours
-- GROUP BY + HAVING + ORDER BY
SELECT pickup_hour,
       COUNT(*)                        AS trips,
       ROUND(AVG(total_amount), 2)     AS avg_revenue_per_trip
FROM taxi_trips
GROUP BY pickup_hour
HAVING COUNT(*) > 0
ORDER BY trips DESC;

-- name: weekday_vs_weekend
-- CASE WHEN
SELECT CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
       COUNT(*)                          AS trips,
       ROUND(SUM(total_amount), 2)       AS revenue,
       ROUND(AVG(trip_distance), 2)      AS avg_distance
FROM taxi_trips
GROUP BY CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END;

-- name: peak_hour_share
-- CTE + RANK window: share of all trips that happen in the 5 busiest hours
WITH hourly AS (
    SELECT pickup_hour, COUNT(*) AS trips FROM taxi_trips GROUP BY pickup_hour
), ranked AS (
    SELECT pickup_hour, trips, RANK() OVER (ORDER BY trips DESC) AS hour_rank FROM hourly
)
SELECT ROUND(100.0 * SUM(CASE WHEN hour_rank <= 5 THEN trips ELSE 0 END) / SUM(trips), 2)
       AS pct_trips_in_top5_hours
FROM ranked;

-- name: distance_category_summary
SELECT trip_distance_category,
       COUNT(*)                     AS trips,
       ROUND(SUM(total_amount), 2)  AS revenue,
       ROUND(AVG(fare_amount), 2)   AS avg_fare
FROM taxi_trips
GROUP BY trip_distance_category
ORDER BY MIN(trip_distance);
