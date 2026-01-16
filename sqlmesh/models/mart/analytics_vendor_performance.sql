MODEL (
  name mart.analytics_vendor_performance,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column trip_date
  )
);

SELECT
    f.date_key AS trip_date,
    f.vendor_key,
    v.vendor_name,

    -- Trip volume
    COUNT(*) AS total_trips,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY f.date_key) AS market_share_pct,

    -- Passenger metrics
    SUM(f.passenger_count) AS total_passengers,
    AVG(f.passenger_count) AS avg_passengers_per_trip,

    -- Distance metrics
    SUM(f.trip_distance) AS total_distance_miles,
    AVG(f.trip_distance) AS avg_distance_miles,

    -- Duration and speed
    AVG(f.trip_duration_minutes) AS avg_duration_minutes,
    AVG(f.avg_speed_mph) AS avg_speed_mph,

    -- Revenue metrics
    SUM(f.total_amount) AS total_revenue,
    AVG(f.fare_amount) AS avg_fare_amount,
    AVG(f.total_amount) AS avg_total_amount,
    AVG(f.fare_per_mile) AS avg_fare_per_mile,
    AVG(f.revenue_per_minute) AS avg_revenue_per_minute,

    -- Tip metrics
    SUM(f.tip_amount) AS total_tips,
    AVG(f.tip_amount) AS avg_tip_amount,
    AVG(f.tip_percentage) AS avg_tip_percentage,
    SUM(CASE WHEN f.is_high_tip THEN 1 ELSE 0 END) AS high_tip_trips,

    -- Payment method distribution
    SUM(CASE WHEN f.payment_type_key = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS credit_card_pct,
    SUM(CASE WHEN f.payment_type_key = 2 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS cash_pct,

    -- Trip type distribution
    SUM(CASE WHEN f.is_airport_trip THEN 1 ELSE 0 END) AS airport_trips,
    SUM(CASE WHEN f.distance_category = 'Short' THEN 1 ELSE 0 END) AS short_distance_trips,
    SUM(CASE WHEN f.distance_category = 'Medium' THEN 1 ELSE 0 END) AS medium_distance_trips,
    SUM(CASE WHEN f.distance_category = 'Long' THEN 1 ELSE 0 END) AS long_distance_trips

FROM mart.fact_trip f
INNER JOIN mart.dim_vendor v ON f.vendor_key = v.vendor_key
WHERE f.date_key BETWEEN @start_date AND @end_date
GROUP BY
    f.date_key,
    f.vendor_key,
    v.vendor_name
