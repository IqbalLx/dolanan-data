MODEL (
  name intermediate.cleaned_yellow_trip,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column tpep_pickup_datetime
  )
);

SELECT
    -- IDs
    vendorid,

    -- Timestamps
    tpep_pickup_datetime,
    tpep_dropoff_datetime,

    -- Trip details
    passenger_count,
    trip_distance,

    -- Location
    pickup_longitude,
    pickup_latitude,
    dropoff_longitude,
    dropoff_latitude,

    -- Codes
    ratecodeid,
    payment_type,

    -- Flags
    store_and_fwd_flag,

    -- Financial
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount

FROM intermediate.base_yellow_trip

WHERE
    tpep_pickup_datetime BETWEEN @start_date AND @end_date
    -- Data quality filters
    AND tpep_dropoff_datetime IS NOT NULL
    AND tpep_pickup_datetime < tpep_dropoff_datetime
    AND trip_distance > 0
    AND trip_distance < 100  -- Remove outliers (>100 miles unlikely)
    AND fare_amount > 0
    AND fare_amount < 500  -- Remove outliers
    AND total_amount > 0
    AND total_amount < 1000  -- Remove outliers
    AND passenger_count > 0
    AND passenger_count <= 6  -- Yellow cabs max capacity
    AND pickup_latitude BETWEEN 40.5 AND 41.0  -- NYC area
    AND pickup_longitude BETWEEN -74.5 AND -73.5  -- NYC area
