MODEL (
  name nyc_taxi_catalog.mart.mart_hourly_demand_patterns,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column pickup_datetime_hour,
    batch_size 10000
  ),

  cron '@daily',
  grain (pickup_datetime_hour, pickup_zone)
);

-- Hourly demand patterns with trends and comparisons
SELECT
    pickup_datetime_hour,
    pickup_zone,
    pickup_date,
    pickup_hour,
    day_of_week,
    day_type,
    time_category,

    -- Volume metrics
    trip_count,
    vendor_count,
    total_passengers,
    avg_passengers,

    -- Distance and duration
    avg_distance AS avg_distance_miles,
    avg_duration_minutes,

    -- Revenue metrics
    total_revenue,
    avg_revenue_per_trip,
    total_tips,
    avg_tip_percentage,

    -- Location metrics
    avg_pickup_latitude,
    avg_pickup_longitude,

    -- Efficiency metrics
    avg_distance / NULLIF(avg_duration_minutes, 0) * 60 AS avg_speed_mph,
    total_revenue / NULLIF(trip_count, 0) AS revenue_per_trip,
    total_revenue / NULLIF(avg_duration_minutes * trip_count, 0) * 60 AS revenue_per_hour,

    -- Demand intensity score (normalized by hour of day across all locations)
    trip_count / NULLIF(
        AVG(trip_count) OVER (
            PARTITION BY pickup_hour, day_type
        ), 0
    ) AS demand_intensity_score,

    -- Peak hour indicator
    CASE
        WHEN trip_count >= PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY trip_count) OVER (
            PARTITION BY pickup_zone, day_type
        ) THEN true
        ELSE false
    END AS is_peak_hour,

    -- Trip density (trips per passenger)
    trip_count / NULLIF(total_passengers, 0) AS trips_per_passenger,

    -- Average fare per passenger
    total_revenue / NULLIF(total_passengers, 0) AS revenue_per_passenger

FROM nyc_taxi_catalog.intermediate.int_hourly_demand
WHERE pickup_datetime_hour BETWEEN @start_date AND @end_date
