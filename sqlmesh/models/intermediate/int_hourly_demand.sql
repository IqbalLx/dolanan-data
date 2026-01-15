MODEL (
  name nyc_taxi_catalog.intermediate.int_hourly_demand,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column pickup_datetime,
    batch_size 10000
  ),

  cron '@daily',
  grain (pickup_datetime_hour, pickup_zone)
);

-- Hourly demand patterns by pickup location zone
SELECT
    pickup_datetime,
    DATE_TRUNC('hour', pickup_datetime) AS pickup_datetime_hour,

    -- Create pickup zone by binning coordinates (approx 0.01 degree bins ~ 1km)
    CONCAT(
        CAST(FLOOR(pickup_latitude / 0.01) * 0.01 AS VARCHAR),
        ',',
        CAST(FLOOR(pickup_longitude / 0.01) * 0.01 AS VARCHAR)
    ) AS pickup_zone,

    -- Time dimensions
    DATE(pickup_datetime) AS pickup_date,
    HOUR(pickup_datetime) AS pickup_hour,
    DAY_OF_WEEK(pickup_datetime) AS day_of_week,
    CASE
        WHEN DAY_OF_WEEK(pickup_datetime) IN (6, 7) THEN 'weekend'
        ELSE 'weekday'
    END AS day_type,

    -- Trip counts
    COUNT(*) AS trip_count,
    COUNT(DISTINCT vendor_id) AS vendor_count,

    -- Passenger metrics
    SUM(passenger_count) AS total_passengers,
    AVG(passenger_count) AS avg_passengers,

    -- Distance and duration
    AVG(trip_distance) AS avg_distance,
    AVG(trip_duration_minutes) AS avg_duration_minutes,

    -- Revenue metrics
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_revenue_per_trip,
    SUM(tip_amount) AS total_tips,
    AVG(tip_percentage) AS avg_tip_percentage,

    -- Location metrics
    AVG(pickup_latitude) AS avg_pickup_latitude,
    AVG(pickup_longitude) AS avg_pickup_longitude,

    -- Demand indicators
    time_of_day_category AS time_category

FROM nyc_taxi_catalog.intermediate.int_taxi_trips_cleaned
WHERE pickup_datetime BETWEEN @start_date AND @end_date
GROUP BY
    DATE_TRUNC('hour', pickup_datetime),
    CONCAT(
        CAST(FLOOR(pickup_latitude / 0.01) * 0.01 AS VARCHAR),
        ',',
        CAST(FLOOR(pickup_longitude / 0.01) * 0.01 AS VARCHAR)
    ),
    DATE(pickup_datetime),
    HOUR(pickup_datetime),
    DAY_OF_WEEK(pickup_datetime),
    time_of_day_category
