-- name: daily_revenue_moving_avg
-- Window frame: 7-day moving average
WITH daily AS (
    SELECT pickup_date, SUM(total_amount) AS revenue
    FROM taxi_trips
    GROUP BY pickup_date
)
SELECT pickup_date,
       ROUND(revenue, 2) AS revenue,
       ROUND(AVG(revenue) OVER (ORDER BY pickup_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2)
           AS revenue_7day_avg
FROM daily
ORDER BY pickup_date;

-- name: monthly_revenue_growth
-- Date functions + LAG window function
WITH monthly AS (
    SELECT date_format(pickup_date, 'yyyy-MM') AS year_month,
           SUM(total_amount)                   AS revenue
    FROM taxi_trips
    GROUP BY date_format(pickup_date, 'yyyy-MM')
)
SELECT year_month,
       ROUND(revenue, 2) AS revenue,
       ROUND(100.0 * (revenue - LAG(revenue) OVER (ORDER BY year_month))
             / LAG(revenue) OVER (ORDER BY year_month), 2) AS mom_growth_pct
FROM monthly
ORDER BY year_month;

-- name: payment_summary
SELECT payment_name,
       COUNT(*)                     AS trips,
       ROUND(SUM(total_amount), 2)  AS revenue,
       ROUND(AVG(fare_amount), 2)   AS avg_fare,
       ROUND(AVG(tip_amount), 2)    AS avg_tip
FROM taxi_trips
GROUP BY payment_name
ORDER BY trips DESC;

-- name: tip_by_time_period
-- Card trips only: cash tips are not recorded in the TLC data
SELECT time_period,
       COUNT(*)                        AS card_trips,
       ROUND(AVG(tip_percentage), 2)   AS avg_tip_pct
FROM taxi_trips
WHERE payment_type = 1
GROUP BY time_period
ORDER BY avg_tip_pct DESC;
