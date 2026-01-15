MODEL (
  name nyc_taxi_catalog.mart.mart_time_trends,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column pickup_date,
    batch_size 10000
  ),

  cron '@daily',
  grain (pickup_date)
);

-- Time-based trends analysis with rolling averages and growth metrics
SELECT
    pickup_date,

    -- Day attributes
    DAY_OF_WEEK(pickup_date) AS day_of_week,
    DAY_OF_MONTH(pickup_date) AS day_of_month,
    WEEK(pickup_date) AS week_of_year,
    MONTH(pickup_date) AS month,
    QUARTER(pickup_date) AS quarter,
    YEAR(pickup_date) AS year,
    CASE
        WHEN DAY_OF_WEEK(pickup_date) IN (6, 7) THEN 'weekend'
        ELSE 'weekday'
    END AS day_type,

    -- Current day metrics
    COUNT(*) AS total_trips,
    SUM(passenger_count) AS total_passengers,
    SUM(trip_distance) AS total_distance_miles,
    SUM(total_amount) AS total_revenue,
    AVG(trip_distance) AS avg_trip_distance_miles,
    AVG(total_amount) AS avg_revenue_per_trip,
    AVG(trip_duration_minutes) AS avg_trip_duration_minutes,
    AVG(tip_percentage) AS avg_tip_percentage,

    -- 7-day rolling averages
    AVG(COUNT(*)) OVER (
        ORDER BY pickup_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS trips_7day_avg,

    AVG(SUM(total_amount)) OVER (
        ORDER BY pickup_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS revenue_7day_avg,

    AVG(AVG(trip_distance)) OVER (
        ORDER BY pickup_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS distance_7day_avg,

    -- 30-day rolling averages
    AVG(COUNT(*)) OVER (
        ORDER BY pickup_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS trips_30day_avg,

    AVG(SUM(total_amount)) OVER (
        ORDER BY pickup_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS revenue_30day_avg,

    -- Day-over-day growth
    (COUNT(*) - LAG(COUNT(*), 1) OVER (ORDER BY pickup_date)) /
        NULLIF(LAG(COUNT(*), 1) OVER (ORDER BY pickup_date), 0) * 100 AS trips_dod_growth_pct,

    (SUM(total_amount) - LAG(SUM(total_amount), 1) OVER (ORDER BY pickup_date)) /
        NULLIF(LAG(SUM(total_amount), 1) OVER (ORDER BY pickup_date), 0) * 100 AS revenue_dod_growth_pct,

    -- Week-over-week growth (same day previous week)
    (COUNT(*) - LAG(COUNT(*), 7) OVER (ORDER BY pickup_date)) /
        NULLIF(LAG(COUNT(*), 7) OVER (ORDER BY pickup_date), 0) * 100 AS trips_wow_growth_pct,

    (SUM(total_amount) - LAG(SUM(total_amount), 7) OVER (ORDER BY pickup_date)) /
        NULLIF(LAG(SUM(total_amount), 7) OVER (ORDER BY pickup_date), 0) * 100 AS revenue_wow_growth_pct,

    -- Month-over-month growth (same day previous month, approximately)
    (COUNT(*) - LAG(COUNT(*), 30) OVER (ORDER BY pickup_date)) /
        NULLIF(LAG(COUNT(*), 30) OVER (ORDER BY pickup_date), 0) * 100 AS trips_mom_growth_pct,

    (SUM(total_amount) - LAG(SUM(total_amount), 30) OVER (ORDER BY pickup_date)) /
        NULLIF(LAG(SUM(total_amount), 30) OVER (ORDER BY pickup_date), 0) * 100 AS revenue_mom_growth_pct,

    -- Peak performance indicators
    COUNT(*) >= PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY COUNT(*)) OVER () AS is_high_volume_day,
    SUM(total_amount) >= PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY SUM(total_amount)) OVER () AS is_high_revenue_day,

    -- Time category distributions
    SUM(CASE WHEN time_of_day_category = 'morning_rush' THEN 1 ELSE 0 END) AS morning_rush_trips,
    SUM(CASE WHEN time_of_day_category = 'evening_rush' THEN 1 ELSE 0 END) AS evening_rush_trips,
    SUM(CASE WHEN time_of_day_category = 'late_night' THEN 1 ELSE 0 END) AS late_night_trips,
    SUM(CASE WHEN time_of_day_category = 'off_peak' THEN 1 ELSE 0 END) AS off_peak_trips,

    -- Efficiency trends
    AVG(trip_distance / NULLIF(trip_duration_minutes, 0) * 60) AS avg_speed_mph,
    SUM(total_amount) / NULLIF(SUM(trip_distance), 0) AS revenue_per_mile,

    -- Passenger utilization
    SUM(passenger_count) / NULLIF(COUNT(*), 0) AS avg_passengers_per_trip,

    -- Payment mix
    SUM(CASE WHEN payment_type = 1 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100 AS credit_card_percentage

FROM nyc_taxi_catalog.intermediate.int_taxi_trips_cleaned
WHERE pickup_date BETWEEN @start_date AND @end_date
GROUP BY pickup_date
