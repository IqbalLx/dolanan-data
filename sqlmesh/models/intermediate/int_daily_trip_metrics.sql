MODEL (
  name nyc_taxi_catalog.intermediate.int_daily_trip_metrics,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column pickup_date,
    batch_size 10000
  ),

  cron '@daily',
  grain (pickup_date, pickup_zone)
);

-- Daily aggregated trip metrics by pickup location zone
SELECT
    pickup_date,

    -- Create pickup zone by binning coordinates (approx 0.01 degree bins ~ 1km)
    CONCAT(
        CAST(FLOOR(pickup_latitude / 0.01) * 0.01 AS VARCHAR),
        ',',
        CAST(FLOOR(pickup_longitude / 0.01) * 0.01 AS VARCHAR)
    ) AS pickup_zone,

    -- Trip counts
    COUNT(*) AS total_trips,
    COUNT(DISTINCT vendor_id) AS unique_vendors,

    -- Passenger metrics
    SUM(passenger_count) AS total_passengers,
    AVG(passenger_count) AS avg_passengers_per_trip,

    -- Distance metrics
    SUM(trip_distance) AS total_distance,
    AVG(trip_distance) AS avg_trip_distance,
    MIN(trip_distance) AS min_trip_distance,
    MAX(trip_distance) AS max_trip_distance,

    -- Duration metrics
    AVG(trip_duration_minutes) AS avg_trip_duration_minutes,
    MIN(trip_duration_minutes) AS min_trip_duration_minutes,
    MAX(trip_duration_minutes) AS max_trip_duration_minutes,

    -- Revenue metrics
    SUM(fare_amount) AS total_fare_amount,
    SUM(tip_amount) AS total_tip_amount,
    SUM(total_amount) AS total_revenue,
    AVG(fare_amount) AS avg_fare_amount,
    AVG(tip_amount) AS avg_tip_amount,
    AVG(total_amount) AS avg_total_amount,

    -- Trip category breakdowns
    SUM(CASE WHEN trip_distance_category = 'short' THEN 1 ELSE 0 END) AS short_trips,
    SUM(CASE WHEN trip_distance_category = 'medium' THEN 1 ELSE 0 END) AS medium_trips,
    SUM(CASE WHEN trip_distance_category = 'long' THEN 1 ELSE 0 END) AS long_trips,
    SUM(CASE WHEN trip_distance_category = 'very_long' THEN 1 ELSE 0 END) AS very_long_trips,

    -- Time category breakdowns
    SUM(CASE WHEN time_of_day_category = 'morning_rush' THEN 1 ELSE 0 END) AS morning_rush_trips,
    SUM(CASE WHEN time_of_day_category = 'evening_rush' THEN 1 ELSE 0 END) AS evening_rush_trips,
    SUM(CASE WHEN time_of_day_category = 'late_night' THEN 1 ELSE 0 END) AS late_night_trips,
    SUM(CASE WHEN time_of_day_category = 'off_peak' THEN 1 ELSE 0 END) AS off_peak_trips,

    -- Payment type breakdown
    SUM(CASE WHEN payment_type = 1 THEN 1 ELSE 0 END) AS credit_card_trips,
    SUM(CASE WHEN payment_type = 2 THEN 1 ELSE 0 END) AS cash_trips,

    -- Efficiency metrics
    AVG(trip_distance / NULLIF(trip_duration_minutes, 0) * 60) AS avg_speed_mph,

    -- Average coordinates for the zone
    AVG(pickup_latitude) AS avg_pickup_latitude,
    AVG(pickup_longitude) AS avg_pickup_longitude

FROM nyc_taxi_catalog.intermediate.int_taxi_trips_cleaned
WHERE pickup_date BETWEEN @start_date AND @end_date
GROUP BY
    pickup_date,
    CONCAT(
        CAST(FLOOR(pickup_latitude / 0.01) * 0.01 AS VARCHAR),
        ',',
        CAST(FLOOR(pickup_longitude / 0.01) * 0.01 AS VARCHAR)
    )
