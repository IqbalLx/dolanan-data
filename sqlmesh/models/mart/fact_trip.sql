MODEL (
  name mart.fact_trip,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column pickup_datetime
  )
);

SELECT
    -- Surrogate key
    MD5(CONCAT(
        CAST(tpep_pickup_datetime AS TEXT),
        CAST(vendorid AS TEXT),
        CAST(pickup_latitude AS TEXT),
        CAST(pickup_longitude AS TEXT)
    )) AS trip_key,

    -- Dimension keys (matching dimension table primary keys)
    pickup_date AS date_key,
    pickup_hour AS time_key,
    vendorid AS vendor_key,
    payment_type AS payment_type_key,
    ratecodeid AS rate_code_key,

    -- Timestamps
    tpep_pickup_datetime AS pickup_datetime,
    tpep_dropoff_datetime AS dropoff_datetime,

    -- Trip details
    passenger_count,
    trip_distance,
    distance_category,
    duration_category,

    -- Location
    pickup_latitude,
    pickup_longitude,
    dropoff_latitude,
    dropoff_longitude,

    -- Flags
    store_and_fwd_flag,
    is_rush_hour,
    is_weekend,
    is_airport_trip,
    is_high_tip,

    -- Financial metrics
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,

    -- Calculated metrics (from intermediate layer)
    trip_duration_minutes,
    avg_speed_mph,
    fare_per_mile,
    tip_percentage,
    revenue_per_minute

FROM intermediate.enriched_yellow_trip
WHERE tpep_pickup_datetime BETWEEN @start_date AND @end_date
