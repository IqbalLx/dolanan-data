MODEL (
  name nyc_taxi_catalog.intermediate.int_taxi_trips_cleaned,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column pickup_datetime,
    batch_size 10000
  ),

  cron '@daily',
  grain (vendor_id, pickup_datetime, dropoff_datetime)
);

-- Cleaned and enriched taxi trip data with data quality filters
SELECT
    -- Identifiers
    vendor_id,

    -- Timestamps
    tpep_pickup_datetime AS pickup_datetime,
    tpep_dropoff_datetime AS dropoff_datetime,

    -- Trip duration in minutes
    DATE_DIFF('minute', tpep_pickup_datetime, tpep_dropoff_datetime) AS trip_duration_minutes,

    -- Date and time components
    DATE(tpep_pickup_datetime) AS pickup_date,
    HOUR(tpep_pickup_datetime) AS pickup_hour,
    DAY_OF_WEEK(tpep_pickup_datetime) AS pickup_day_of_week,

    -- Location coordinates
    pickup_longitude,
    pickup_latitude,
    dropoff_longitude,
    dropoff_latitude,

    -- Trip metrics
    passenger_count,
    trip_distance,

    -- Rate and payment
    rate_code_id,
    store_and_fwd_flag,
    payment_type,

    -- Fare breakdown
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,

    -- Calculated metrics
    total_amount - tip_amount AS fare_without_tip,
    CASE
        WHEN fare_amount > 0 THEN (tip_amount / fare_amount) * 100
        ELSE 0
    END AS tip_percentage,

    -- Trip categorization
    CASE
        WHEN trip_distance <= 1 THEN 'short'
        WHEN trip_distance <= 5 THEN 'medium'
        WHEN trip_distance <= 10 THEN 'long'
        ELSE 'very_long'
    END AS trip_distance_category,

    CASE
        WHEN HOUR(tpep_pickup_datetime) BETWEEN 6 AND 9 THEN 'morning_rush'
        WHEN HOUR(tpep_pickup_datetime) BETWEEN 17 AND 20 THEN 'evening_rush'
        WHEN HOUR(tpep_pickup_datetime) >= 22 OR HOUR(tpep_pickup_datetime) <= 5 THEN 'late_night'
        ELSE 'off_peak'
    END AS time_of_day_category,

    CASE
        WHEN DAY_OF_WEEK(tpep_pickup_datetime) IN (6, 7) THEN 'weekend'
        ELSE 'weekday'
    END AS day_type

FROM nyc_taxi_catalog.raw.yellow_tripdata
WHERE
    -- Data quality filters
    tpep_pickup_datetime IS NOT NULL
    AND tpep_dropoff_datetime IS NOT NULL
    AND tpep_pickup_datetime BETWEEN @start_date AND @end_date
    AND tpep_dropoff_datetime > tpep_pickup_datetime
    AND passenger_count > 0
    AND passenger_count <= 8
    AND trip_distance > 0
    AND trip_distance <= 100
    AND fare_amount > 0
    AND fare_amount <= 500
    AND total_amount > 0
    AND total_amount <= 1000
    AND pickup_longitude BETWEEN -74.05 AND -73.75
    AND pickup_latitude BETWEEN 40.6 AND 40.9
    AND dropoff_longitude BETWEEN -74.05 AND -73.75
    AND dropoff_latitude BETWEEN 40.6 AND 40.9
