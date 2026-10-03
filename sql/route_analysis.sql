-- name: top_routes_by_revenue
-- CTE + RANK window function
WITH routes AS (
    SELECT pickup_zone, dropoff_zone,
           COUNT(*) AS trips,
           SUM(total_amount) AS total_revenue
    FROM taxi_trips
    GROUP BY pickup_zone, dropoff_zone
), ranked AS (
    SELECT *, RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank FROM routes
)
SELECT revenue_rank, pickup_zone, dropoff_zone, trips, ROUND(total_revenue, 2) AS total_revenue
FROM ranked
WHERE revenue_rank <= 20
ORDER BY revenue_rank;

-- name: busiest_pickup_zones
-- Window over an aggregate: share of all trips
SELECT pickup_zone,
       COUNT(*) AS trips,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_all_trips
FROM taxi_trips
GROUP BY pickup_zone
ORDER BY trips DESC
LIMIT 20;

-- name: highest_fare_routes
-- HAVING removes rare routes with unreliable averages
SELECT pickup_zone, dropoff_zone,
       COUNT(*) AS trips,
       ROUND(AVG(fare_amount), 2) AS avg_fare
FROM taxi_trips
GROUP BY pickup_zone, dropoff_zone
HAVING COUNT(*) >= 20
ORDER BY avg_fare DESC
LIMIT 20;
