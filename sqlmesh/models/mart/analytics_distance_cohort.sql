MODEL (
  name mart.analytics_distance_cohort,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column trip_date
  )
);

SELECT
    f.date_key AS trip_date,
    f.distance_category,

    -- Trip volume
    COUNT(*) AS total_trips,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY f.date_key) AS trip_share_pct,

    -- Passenger metrics
    SUM(f.passenger_count) AS total_passengers,
    AVG(f.passenger_count) AS avg_passengers_per_trip,

    -- Distance metrics
    SUM(f.trip_distance) AS total_distance_miles,
    AVG(f.trip_distance) AS avg_distance_miles,
    MIN(f.trip_distance) AS min_distance_miles,
    MAX(f.trip_distance) AS max_distance_miles,

    -- Duration metrics
    AVG(f.trip_duration_minutes) AS avg_duration_minutes,
    AVG(f.avg_speed_mph) AS avg_speed_mph,

    -- Revenue metrics
    SUM(f.total_amount) AS total_revenue,
    AVG(f.fare_amount) AS avg_fare_amount,
    AVG(f.total_amount) AS avg_total_amount,
    AVG(f.fare_per_mile) AS avg_fare_per_mile,
    AVG(f.revenue_per_minute) AS avg_revenue_per_minute,

    -- Tip metrics
    AVG(f.tip_amount) AS avg_tip_amount,
    AVG(f.tip_percentage) AS avg_tip_percentage,
    SUM(CASE WHEN f.is_high_tip THEN 1 ELSE 0 END) AS high_tip_trips,

    -- Time distribution
    SUM(CASE WHEN f.is_rush_hour THEN 1 ELSE 0 END) AS rush_hour_trips,
    SUM(CASE WHEN f.is_weekend THEN 1 ELSE 0 END) AS weekend_trips,

    -- Payment distribution
    SUM(CASE WHEN f.payment_type_key = 1 THEN 1 ELSE 0 END) AS credit_card_trips,
    SUM(CASE WHEN f.payment_type_key = 2 THEN 1 ELSE 0 END) AS cash_trips,

    -- Special trip types
    SUM(CASE WHEN f.is_airport_trip THEN 1 ELSE 0 END) AS airport_trips

FROM mart.fact_trip f
WHERE f.date_key BETWEEN @start_date AND @end_date
GROUP BY
    f.date_key,
    f.distance_category
