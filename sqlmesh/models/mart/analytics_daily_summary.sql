MODEL (
  name mart.analytics_daily_summary,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column trip_date
  )
);

SELECT
    f.date_key AS trip_date,
    d.day_name,
    d.is_weekend,

    -- Trip volume
    COUNT(*) AS total_trips,
    COUNT(DISTINCT f.vendor_key) AS active_vendors,

    -- Passenger metrics
    SUM(f.passenger_count) AS total_passengers,
    AVG(f.passenger_count) AS avg_passengers_per_trip,

    -- Distance metrics
    SUM(f.trip_distance) AS total_distance_miles,
    AVG(f.trip_distance) AS avg_distance_miles,

    -- Duration metrics
    AVG(f.trip_duration_minutes) AS avg_duration_minutes,
    AVG(f.avg_speed_mph) AS avg_speed_mph,

    -- Revenue metrics
    SUM(f.fare_amount) AS total_fare_amount,
    SUM(f.tip_amount) AS total_tip_amount,
    SUM(f.total_amount) AS total_revenue,
    AVG(f.fare_amount) AS avg_fare_amount,
    AVG(f.tip_amount) AS avg_tip_amount,
    AVG(f.total_amount) AS avg_total_amount,
    AVG(f.revenue_per_minute) AS avg_revenue_per_minute,

    -- Tip metrics
    AVG(f.tip_percentage) AS avg_tip_percentage,
    SUM(CASE WHEN f.tip_amount > 0 THEN 1 ELSE 0 END) AS trips_with_tips,
    SUM(CASE WHEN f.is_high_tip THEN 1 ELSE 0 END) AS high_tip_trips,

    -- Payment distribution
    SUM(CASE WHEN f.payment_type_key = 1 THEN 1 ELSE 0 END) AS credit_card_trips,
    SUM(CASE WHEN f.payment_type_key = 2 THEN 1 ELSE 0 END) AS cash_trips,

    -- Rate code distribution
    SUM(CASE WHEN f.rate_code_key = 1 THEN 1 ELSE 0 END) AS standard_rate_trips,
    SUM(CASE WHEN f.rate_code_key = 2 THEN 1 ELSE 0 END) AS jfk_trips,
    SUM(CASE WHEN f.is_airport_trip THEN 1 ELSE 0 END) AS airport_trips,

    -- Trip categories
    SUM(CASE WHEN f.distance_category = 'Short' THEN 1 ELSE 0 END) AS short_distance_trips,
    SUM(CASE WHEN f.distance_category = 'Medium' THEN 1 ELSE 0 END) AS medium_distance_trips,
    SUM(CASE WHEN f.distance_category = 'Long' THEN 1 ELSE 0 END) AS long_distance_trips

FROM mart.fact_trip f
INNER JOIN mart.dim_date d ON f.date_key = d.date_key
WHERE f.date_key BETWEEN @start_date AND @end_date
GROUP BY
    f.date_key,
    d.day_name,
    d.is_weekend
