MODEL (
  name nyc_taxi_catalog.mart.mart_daily_summary,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column pickup_date,
    batch_size 10000
  ),

  cron '@daily',
  grain (pickup_date)
);

-- Daily summary of NYC taxi trips
SELECT
    pickup_date,

    -- Trip volume metrics
    SUM(total_trips) AS total_trips,
    COUNT(DISTINCT pickup_zone) AS active_pickup_zones,
    SUM(unique_vendors) AS total_vendor_activities,

    -- Passenger metrics
    SUM(total_passengers) AS total_passengers,
    AVG(avg_passengers_per_trip) AS avg_passengers_per_trip,

    -- Distance metrics
    SUM(total_distance) AS total_distance_miles,
    AVG(avg_trip_distance) AS avg_trip_distance_miles,
    MAX(max_trip_distance) AS longest_trip_distance_miles,

    -- Duration metrics
    AVG(avg_trip_duration_minutes) AS avg_trip_duration_minutes,
    MAX(max_trip_duration_minutes) AS longest_trip_duration_minutes,

    -- Revenue metrics
    SUM(total_fare_amount) AS total_fare_amount,
    SUM(total_tip_amount) AS total_tip_amount,
    SUM(total_revenue) AS total_revenue,
    AVG(avg_fare_amount) AS avg_fare_per_trip,
    AVG(avg_tip_amount) AS avg_tip_per_trip,
    AVG(avg_total_amount) AS avg_revenue_per_trip,
    SUM(total_tip_amount) / NULLIF(SUM(total_fare_amount), 0) * 100 AS overall_tip_percentage,

    -- Trip category distribution
    SUM(short_trips) AS short_trips,
    SUM(medium_trips) AS medium_trips,
    SUM(long_trips) AS long_trips,
    SUM(very_long_trips) AS very_long_trips,

    -- Time category distribution
    SUM(morning_rush_trips) AS morning_rush_trips,
    SUM(evening_rush_trips) AS evening_rush_trips,
    SUM(late_night_trips) AS late_night_trips,
    SUM(off_peak_trips) AS off_peak_trips,

    -- Payment distribution
    SUM(credit_card_trips) AS credit_card_trips,
    SUM(cash_trips) AS cash_trips,
    SUM(credit_card_trips) / NULLIF(SUM(total_trips), 0) * 100 AS credit_card_percentage,

    -- Efficiency metrics
    AVG(avg_speed_mph) AS avg_speed_mph,
    SUM(total_revenue) / NULLIF(SUM(total_trips), 0) AS revenue_per_trip,
    SUM(total_revenue) / NULLIF(SUM(total_distance), 0) AS revenue_per_mile,

    -- Day type
    CASE
        WHEN DAY_OF_WEEK(pickup_date) IN (6, 7) THEN 'weekend'
        ELSE 'weekday'
    END AS day_type

FROM nyc_taxi_catalog.intermediate.int_daily_trip_metrics
WHERE pickup_date BETWEEN @start_date AND @end_date
GROUP BY pickup_date
