MODEL (
  name nyc_taxi_catalog.intermediate.int_location_pairs,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column pickup_date,
    batch_size 10000
  ),

  cron '@daily',
  grain (pickup_date, pickup_zone, dropoff_zone)
);

-- Location pair (route) analysis aggregated by day using coordinate zones
SELECT
    pickup_date,

    -- Create pickup zone by binning coordinates (approx 0.01 degree bins ~ 1km)
    CONCAT(
        CAST(FLOOR(pickup_latitude / 0.01) * 0.01 AS VARCHAR),
        ',',
        CAST(FLOOR(pickup_longitude / 0.01) * 0.01 AS VARCHAR)
    ) AS pickup_zone,

    -- Create dropoff zone by binning coordinates (approx 0.01 degree bins ~ 1km)
    CONCAT(
        CAST(FLOOR(dropoff_latitude / 0.01) * 0.01 AS VARCHAR),
        ',',
        CAST(FLOOR(dropoff_longitude / 0.01) * 0.01 AS VARCHAR)
    ) AS dropoff_zone,

    -- Trip counts
    COUNT(*) AS total_trips,

    -- Passenger metrics
    SUM(passenger_count) AS total_passengers,
    AVG(passenger_count) AS avg_passengers,

    -- Distance metrics
    AVG(trip_distance) AS avg_distance,
    MIN(trip_distance) AS min_distance,
    MAX(trip_distance) AS max_distance,
    STDDEV(trip_distance) AS stddev_distance,

    -- Duration metrics
    AVG(trip_duration_minutes) AS avg_duration_minutes,
    MIN(trip_duration_minutes) AS min_duration_minutes,
    MAX(trip_duration_minutes) AS max_duration_minutes,
    STDDEV(trip_duration_minutes) AS stddev_duration_minutes,

    -- Revenue metrics
    AVG(fare_amount) AS avg_fare,
    MIN(fare_amount) AS min_fare,
    MAX(fare_amount) AS max_fare,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_revenue_per_trip,

    -- Tip metrics
    AVG(tip_amount) AS avg_tip,
    AVG(tip_percentage) AS avg_tip_percentage,
    SUM(CASE WHEN payment_type = 1 THEN 1 ELSE 0 END) AS credit_card_trips,
    SUM(CASE WHEN payment_type = 2 THEN 1 ELSE 0 END) AS cash_trips,

    -- Efficiency metrics
    AVG(trip_distance / NULLIF(trip_duration_minutes, 0) * 60) AS avg_speed_mph,
    AVG(fare_amount / NULLIF(trip_distance, 0)) AS avg_fare_per_mile,

    -- Time category breakdowns
    SUM(CASE WHEN time_of_day_category = 'morning_rush' THEN 1 ELSE 0 END) AS morning_rush_trips,
    SUM(CASE WHEN time_of_day_category = 'evening_rush' THEN 1 ELSE 0 END) AS evening_rush_trips,
    SUM(CASE WHEN time_of_day_category = 'late_night' THEN 1 ELSE 0 END) AS late_night_trips,
    SUM(CASE WHEN time_of_day_category = 'off_peak' THEN 1 ELSE 0 END) AS off_peak_trips,

    -- Day type breakdown
    SUM(CASE WHEN day_type = 'weekend' THEN 1 ELSE 0 END) AS weekend_trips,
    SUM(CASE WHEN day_type = 'weekday' THEN 1 ELSE 0 END) AS weekday_trips,

    -- Average coordinates for zones
    AVG(pickup_latitude) AS avg_pickup_latitude,
    AVG(pickup_longitude) AS avg_pickup_longitude,
    AVG(dropoff_latitude) AS avg_dropoff_latitude,
    AVG(dropoff_longitude) AS avg_dropoff_longitude

FROM nyc_taxi_catalog.intermediate.int_taxi_trips_cleaned
WHERE pickup_date BETWEEN @start_date AND @end_date
GROUP BY
    pickup_date,
    CONCAT(
        CAST(FLOOR(pickup_latitude / 0.01) * 0.01 AS VARCHAR),
        ',',
        CAST(FLOOR(pickup_longitude / 0.01) * 0.01 AS VARCHAR)
    ),
    CONCAT(
        CAST(FLOOR(dropoff_latitude / 0.01) * 0.01 AS VARCHAR),
        ',',
        CAST(FLOOR(dropoff_longitude / 0.01) * 0.01 AS VARCHAR)
    )
