MODEL (
  name mart.analytics_payment_method,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column trip_date
  )
);

SELECT
    f.date_key AS trip_date,
    f.payment_type_key,
    p.payment_type_name,

    -- Trip volume
    COUNT(*) AS total_trips,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY f.date_key) AS payment_method_share_pct,

    -- Passenger metrics
    AVG(f.passenger_count) AS avg_passengers_per_trip,

    -- Distance metrics
    AVG(f.trip_distance) AS avg_distance_miles,

    -- Duration
    AVG(f.trip_duration_minutes) AS avg_duration_minutes,

    -- Revenue metrics
    SUM(f.total_amount) AS total_revenue,
    AVG(f.fare_amount) AS avg_fare_amount,
    AVG(f.total_amount) AS avg_total_amount,
    AVG(f.revenue_per_minute) AS avg_revenue_per_minute,

    -- Tip metrics (tips are typically only recorded for credit card payments)
    SUM(f.tip_amount) AS total_tips,
    AVG(f.tip_amount) AS avg_tip_amount,
    AVG(f.tip_percentage) AS avg_tip_percentage,
    SUM(CASE WHEN f.tip_amount > 0 THEN 1 ELSE 0 END) AS trips_with_tips,
    SUM(CASE WHEN f.tip_amount > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS tip_rate_pct,
    SUM(CASE WHEN f.is_high_tip THEN 1 ELSE 0 END) AS high_tip_trips,

    -- Additional charges
    AVG(f.extra) AS avg_extra_charges,
    AVG(f.tolls_amount) AS avg_tolls,

    -- Trip categories
    SUM(CASE WHEN f.distance_category = 'Short' THEN 1 ELSE 0 END) AS short_distance_trips,
    SUM(CASE WHEN f.distance_category = 'Medium' THEN 1 ELSE 0 END) AS medium_distance_trips,
    SUM(CASE WHEN f.distance_category = 'Long' THEN 1 ELSE 0 END) AS long_distance_trips,
    SUM(CASE WHEN f.is_airport_trip THEN 1 ELSE 0 END) AS airport_trips

FROM mart.fact_trip f
INNER JOIN mart.dim_payment_type p ON f.payment_type_key = p.payment_type_key
WHERE f.date_key BETWEEN @start_date AND @end_date
GROUP BY
    f.date_key,
    f.payment_type_key,
    p.payment_type_name
