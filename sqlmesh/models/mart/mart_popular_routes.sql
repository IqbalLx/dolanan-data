MODEL (
  name nyc_taxi_catalog.mart.mart_popular_routes,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column pickup_date,
    batch_size 10000
  ),

  cron '@daily',
  grain (pickup_date, pickup_zone, dropoff_zone)
);

-- Popular routes (pickup-dropoff zone pairs) with performance metrics
SELECT
    pickup_date,
    pickup_zone,
    dropoff_zone,

    -- Volume metrics
    total_trips,
    total_passengers,
    avg_passengers,

    -- Distance metrics
    avg_distance AS avg_distance_miles,
    min_distance AS min_distance_miles,
    max_distance AS max_distance_miles,
    stddev_distance AS stddev_distance_miles,

    -- Duration metrics
    avg_duration_minutes,
    min_duration_minutes,
    max_duration_minutes,
    stddev_duration_minutes,

    -- Revenue metrics
    avg_fare,
    min_fare,
    max_fare,
    total_revenue,
    avg_revenue_per_trip,

    -- Tip metrics
    avg_tip,
    avg_tip_percentage,
    credit_card_trips,
    cash_trips,
    credit_card_trips / NULLIF(total_trips, 0) * 100 AS credit_card_percentage,

    -- Efficiency metrics
    avg_speed_mph,
    avg_fare_per_mile,
    total_revenue / NULLIF(avg_duration_minutes * total_trips, 0) * 60 AS revenue_per_hour,

    -- Time distribution
    morning_rush_trips,
    evening_rush_trips,
    late_night_trips,
    off_peak_trips,
    morning_rush_trips / NULLIF(total_trips, 0) * 100 AS morning_rush_percentage,
    evening_rush_trips / NULLIF(total_trips, 0) * 100 AS evening_rush_percentage,

    -- Day type distribution
    weekend_trips,
    weekday_trips,
    weekend_trips / NULLIF(total_trips, 0) * 100 AS weekend_percentage,

    -- Route popularity rank by volume
    RANK() OVER (
        PARTITION BY pickup_date
        ORDER BY total_trips DESC
    ) AS route_popularity_rank_by_trips,

    -- Route popularity rank by revenue
    RANK() OVER (
        PARTITION BY pickup_date
        ORDER BY total_revenue DESC
    ) AS route_popularity_rank_by_revenue,

    -- Route consistency score (inverse of distance stddev)
    CASE
        WHEN stddev_distance > 0 THEN avg_distance / stddev_distance
        ELSE 0
    END AS route_consistency_score,

    -- Premium route indicator (high fare per mile)
    CASE
        WHEN avg_fare_per_mile >= PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY avg_fare_per_mile) OVER (
            PARTITION BY pickup_date
        ) THEN true
        ELSE false
    END AS is_premium_route,

    -- High volume route indicator
    CASE
        WHEN total_trips >= PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY total_trips) OVER (
            PARTITION BY pickup_date
        ) THEN true
        ELSE false
    END AS is_high_volume_route,

    -- Day type
    CASE
        WHEN DAY_OF_WEEK(pickup_date) IN (6, 7) THEN 'weekend'
        ELSE 'weekday'
    END AS day_type,

    -- Zone coordinates
    avg_pickup_latitude,
    avg_pickup_longitude,
    avg_dropoff_latitude,
    avg_dropoff_longitude

FROM nyc_taxi_catalog.intermediate.int_location_pairs
WHERE pickup_date BETWEEN @start_date AND @end_date
