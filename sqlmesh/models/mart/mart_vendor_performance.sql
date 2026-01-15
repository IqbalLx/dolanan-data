MODEL (
  name nyc_taxi_catalog.mart.mart_vendor_performance,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column pickup_date,
    batch_size 10000
  ),

  cron '@daily',
  grain (pickup_date, vendor_id)
);

-- Vendor performance comparison and analysis
SELECT
    pickup_date,
    vendor_id,

    -- Day attributes
    DAY_OF_WEEK(pickup_date) AS day_of_week,
    CASE
        WHEN DAY_OF_WEEK(pickup_date) IN (6, 7) THEN 'weekend'
        ELSE 'weekday'
    END AS day_type,

    -- Volume metrics
    COUNT(*) AS total_trips,

    -- Passenger metrics
    SUM(passenger_count) AS total_passengers,
    AVG(passenger_count) AS avg_passengers_per_trip,

    -- Distance metrics
    SUM(trip_distance) AS total_distance_miles,
    AVG(trip_distance) AS avg_trip_distance_miles,
    MIN(trip_distance) AS min_trip_distance_miles,
    MAX(trip_distance) AS max_trip_distance_miles,

    -- Duration metrics
    AVG(trip_duration_minutes) AS avg_trip_duration_minutes,
    MIN(trip_duration_minutes) AS min_trip_duration_minutes,
    MAX(trip_duration_minutes) AS max_trip_duration_minutes,

    -- Revenue metrics
    SUM(fare_amount) AS total_fare_amount,
    SUM(tip_amount) AS total_tip_amount,
    SUM(total_amount) AS total_revenue,
    AVG(fare_amount) AS avg_fare_per_trip,
    AVG(tip_amount) AS avg_tip_per_trip,
    AVG(total_amount) AS avg_revenue_per_trip,

    -- Tip analysis
    SUM(tip_amount) / NULLIF(SUM(fare_amount), 0) * 100 AS avg_tip_percentage,
    SUM(CASE WHEN tip_amount > 0 THEN 1 ELSE 0 END) AS trips_with_tips,
    SUM(CASE WHEN tip_amount > 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100 AS tip_rate_percentage,

    -- Payment type distribution
    SUM(CASE WHEN payment_type = 1 THEN 1 ELSE 0 END) AS credit_card_trips,
    SUM(CASE WHEN payment_type = 2 THEN 1 ELSE 0 END) AS cash_trips,
    SUM(CASE WHEN payment_type = 1 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100 AS credit_card_percentage,

    -- Trip distance category distribution
    SUM(CASE WHEN trip_distance_category = 'short' THEN 1 ELSE 0 END) AS short_trips,
    SUM(CASE WHEN trip_distance_category = 'medium' THEN 1 ELSE 0 END) AS medium_trips,
    SUM(CASE WHEN trip_distance_category = 'long' THEN 1 ELSE 0 END) AS long_trips,
    SUM(CASE WHEN trip_distance_category = 'very_long' THEN 1 ELSE 0 END) AS very_long_trips,

    -- Time category distribution
    SUM(CASE WHEN time_of_day_category = 'morning_rush' THEN 1 ELSE 0 END) AS morning_rush_trips,
    SUM(CASE WHEN time_of_day_category = 'evening_rush' THEN 1 ELSE 0 END) AS evening_rush_trips,
    SUM(CASE WHEN time_of_day_category = 'late_night' THEN 1 ELSE 0 END) AS late_night_trips,
    SUM(CASE WHEN time_of_day_category = 'off_peak' THEN 1 ELSE 0 END) AS off_peak_trips,

    -- Efficiency metrics
    AVG(trip_distance / NULLIF(trip_duration_minutes, 0) * 60) AS avg_speed_mph,
    SUM(total_amount) / NULLIF(COUNT(*), 0) AS revenue_per_trip,
    SUM(total_amount) / NULLIF(SUM(trip_distance), 0) AS revenue_per_mile,
    SUM(total_amount) / NULLIF(SUM(trip_duration_minutes), 0) * 60 AS revenue_per_hour,
    SUM(trip_distance) / NULLIF(COUNT(*), 0) AS distance_per_trip,

    -- Market share metrics (percentage of daily trips)
    COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (PARTITION BY pickup_date), 0) * 100 AS daily_market_share_percentage,
    SUM(total_amount) / NULLIF(SUM(SUM(total_amount)) OVER (PARTITION BY pickup_date), 0) * 100 AS daily_revenue_share_percentage,

    -- Service quality indicators
    SUM(CASE WHEN trip_duration_minutes <= 30 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100 AS under_30min_trip_percentage,

    -- Vendor ranking by trips
    RANK() OVER (
        PARTITION BY pickup_date
        ORDER BY COUNT(*) DESC
    ) AS vendor_rank_by_trips,

    -- Vendor ranking by revenue
    RANK() OVER (
        PARTITION BY pickup_date
        ORDER BY SUM(total_amount) DESC
    ) AS vendor_rank_by_revenue,

    -- Premium service indicator (high average fare)
    CASE
        WHEN AVG(fare_amount) >= PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY AVG(fare_amount)) OVER (
            PARTITION BY pickup_date
        ) THEN true
        ELSE false
    END AS is_premium_service

FROM nyc_taxi_catalog.intermediate.int_taxi_trips_cleaned
WHERE pickup_date BETWEEN @start_date AND @end_date
GROUP BY pickup_date, vendor_id
