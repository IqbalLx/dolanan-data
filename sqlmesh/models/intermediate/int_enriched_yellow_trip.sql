MODEL (
  name intermediate.enriched_yellow_trip,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column tpep_pickup_datetime
  )
);

SELECT
    -- Surrogate key components
    vendorid,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,

    -- Dimension key components
    DATE(tpep_pickup_datetime) AS pickup_date,
    EXTRACT(HOUR FROM tpep_pickup_datetime) AS pickup_hour,
    ratecodeid,
    payment_type,

    -- Trip details
    passenger_count,
    trip_distance,

    -- Location
    pickup_latitude,
    pickup_longitude,
    dropoff_latitude,
    dropoff_longitude,

    -- Flags
    store_and_fwd_flag,

    -- Financial base amounts
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,

    -- Calculated: Trip duration
    EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60 AS trip_duration_minutes,

    -- Calculated: Speed
    CASE
        WHEN EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) > 0
        THEN trip_distance / (EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 3600)
        ELSE 0
    END AS avg_speed_mph,

    -- Calculated: Fare per mile
    CASE
        WHEN trip_distance > 0
        THEN fare_amount / trip_distance
        ELSE 0
    END AS fare_per_mile,

    -- Calculated: Tip percentage
    CASE
        WHEN fare_amount > 0
        THEN tip_amount / fare_amount
        ELSE 0
    END AS tip_percentage,

    -- Calculated: Revenue per minute
    CASE
        WHEN EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60 > 0
        THEN total_amount / (EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60)
        ELSE 0
    END AS revenue_per_minute,

    -- Categorization: Distance category
    CASE
        WHEN trip_distance <= 1 THEN 'Short'
        WHEN trip_distance <= 5 THEN 'Medium'
        WHEN trip_distance <= 10 THEN 'Long'
        ELSE 'Very Long'
    END AS distance_category,

    -- Categorization: Duration category
    CASE
        WHEN EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60 <= 10 THEN 'Quick'
        WHEN EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60 <= 30 THEN 'Normal'
        WHEN EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60 <= 60 THEN 'Long'
        ELSE 'Very Long'
    END AS duration_category,

    -- Categorization: Time of day
    CASE
        WHEN EXTRACT(HOUR FROM tpep_pickup_datetime) BETWEEN 6 AND 11 THEN 'Morning'
        WHEN EXTRACT(HOUR FROM tpep_pickup_datetime) BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN EXTRACT(HOUR FROM tpep_pickup_datetime) BETWEEN 18 AND 21 THEN 'Evening'
        ELSE 'Night'
    END AS time_of_day,

    -- Flag: Rush hour
    CASE
        WHEN EXTRACT(HOUR FROM tpep_pickup_datetime) BETWEEN 7 AND 9 THEN TRUE
        WHEN EXTRACT(HOUR FROM tpep_pickup_datetime) BETWEEN 17 AND 19 THEN TRUE
        ELSE FALSE
    END AS is_rush_hour,

    -- Flag: Weekend
    CASE
        WHEN EXTRACT(DOW FROM tpep_pickup_datetime) IN (0, 6) THEN TRUE
        ELSE FALSE
    END AS is_weekend,

    -- Flag: Airport trip
    CASE
        WHEN ratecodeid IN (2, 3) THEN TRUE
        ELSE FALSE
    END AS is_airport_trip,

    -- Flag: High tip
    CASE
        WHEN payment_type = 1 AND fare_amount > 0 AND (tip_amount / fare_amount) > 0.20 THEN TRUE
        ELSE FALSE
    END AS is_high_tip

FROM intermediate.cleaned_yellow_trip
WHERE tpep_pickup_datetime BETWEEN @start_date AND @end_date
