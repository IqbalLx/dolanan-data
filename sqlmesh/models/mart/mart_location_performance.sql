MODEL (
  name nyc_taxi_catalog.mart.mart_location_performance,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column pickup_date,
    batch_size 10000
  ),

  cron '@daily',
  grain (pickup_date, pickup_zone)
);

-- Location-level performance metrics for pickup zones
SELECT
    pickup_date,
    pickup_zone,

    -- Volume metrics
    total_trips,
    unique_vendors,
    total_passengers,
    avg_passengers_per_trip,

    -- Distance metrics
    total_distance AS total_distance_miles,
    avg_trip_distance AS avg_trip_distance_miles,
    min_trip_distance AS min_trip_distance_miles,
    max_trip_distance AS max_trip_distance_miles,

    -- Duration metrics
    avg_trip_duration_minutes,
    min_trip_duration_minutes,
    max_trip_duration_minutes,

    -- Revenue metrics
    total_fare_amount,
    total_tip_amount,
    total_revenue,
    avg_fare_amount,
    avg_tip_amount,
    avg_total_amount,
    total_tip_amount / NULLIF(total_fare_amount, 0) * 100 AS tip_percentage,

    -- Trip category distribution
    short_trips,
    medium_trips,
    long_trips,
    very_long_trips,
    short_trips / NULLIF(total_trips, 0) * 100 AS short_trip_percentage,
    medium_trips / NULLIF(total_trips, 0) * 100 AS medium_trip_percentage,
    long_trips / NULLIF(total_trips, 0) * 100 AS long_trip_percentage,

    -- Time category distribution
    morning_rush_trips,
    evening_rush_trips,
    late_night_trips,
    off_peak_trips,
    morning_rush_trips / NULLIF(total_trips, 0) * 100 AS morning_rush_percentage,
    evening_rush_trips / NULLIF(total_trips, 0) * 100 AS evening_rush_percentage,
    late_night_trips / NULLIF(total_trips, 0) * 100 AS late_night_percentage,

    -- Payment distribution
    credit_card_trips,
    cash_trips,
    credit_card_trips / NULLIF(total_trips, 0) * 100 AS credit_card_percentage,

    -- Efficiency metrics
    avg_speed_mph,
    total_revenue / NULLIF(total_trips, 0) AS revenue_per_trip,
    total_revenue / NULLIF(total_distance, 0) AS revenue_per_mile,
    total_revenue / NULLIF(avg_trip_duration_minutes * total_trips, 0) * 60 AS revenue_per_hour,

    -- Utilization score (composite metric)
    (total_trips * avg_passengers_per_trip * avg_trip_distance) /
        NULLIF(max_trip_distance * total_trips, 0) AS utilization_score,

    -- Day type
    CASE
        WHEN DAY_OF_WEEK(pickup_date) IN (6, 7) THEN 'weekend'
        ELSE 'weekday'
    END AS day_type,

    -- Zone coordinates
    avg_pickup_latitude,
    avg_pickup_longitude

FROM nyc_taxi_catalog.intermediate.int_daily_trip_metrics
WHERE pickup_date BETWEEN @start_date AND @end_date
