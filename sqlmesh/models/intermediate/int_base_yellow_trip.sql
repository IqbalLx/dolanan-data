MODEL (
  name intermediate.base_yellow_trip,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column tpep_pickup_datetime
  )
);

WITH

data_2015 AS (
    SELECT
        vendorid,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        passenger_count,
        trip_distance,
        pickup_longitude,
        pickup_latitude,
        ratecodeid,
        store_and_fwd_flag,
        dropoff_longitude,
        dropoff_latitude,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount
    FROM public.yellow_trip_2015
    WHERE tpep_pickup_datetime BETWEEN @start_date AND @end_date
)

, data_2016 AS (
    SELECT
        vendorid,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        passenger_count,
        trip_distance,
        pickup_longitude,
        pickup_latitude,
        ratecodeid,
        store_and_fwd_flag,
        dropoff_longitude,
        dropoff_latitude,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount
    FROM public.yellow_trip_2016
    WHERE tpep_pickup_datetime BETWEEN @start_date AND @end_date
)

, merged_data AS (
    SELECT * FROM data_2015
    UNION ALL
    SELECT * FROM data_2016
)

SELECT * FROM merged_data
