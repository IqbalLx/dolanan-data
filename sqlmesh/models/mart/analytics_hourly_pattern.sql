MODEL (
  name mart.analytics_hourly_pattern,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column trip_date
  )
);

SELECT
    f.date_key AS trip_date,
    f.time_key AS trip_hour,
    t.time_of_day,
    t.is_rush_hour,
    d.is_weekend,

    -- Trip volume
    COUNT(*) AS total_trips,

    -- Passenger metrics
    AVG(f.passenger_count) AS avg_passengers_per_trip,

    -- Distance and duration
    AVG(f.trip_distance) AS avg_distance_miles,
    AVG(f.trip_duration_minutes) AS avg_duration_minutes,
    AVG(f.avg_speed_mph) AS avg_speed_mph,

    -- Revenue metrics
    SUM(f.total_amount) AS total_revenue,
    AVG(f.fare_amount) AS avg_fare_amount,
    AVG(f.tip_amount) AS avg_tip_amount,
    AVG(f.tip_percentage) AS avg_tip_percentage,
    AVG(f.revenue_per_minute) AS avg_revenue_per_minute,

    -- Utilization
    AVG(f.fare_per_mile) AS avg_fare_per_mile,

    -- Trip categories
    SUM(CASE WHEN f.distance_category = 'Short' THEN 1 ELSE 0 END) AS short_distance_trips,
    SUM(CASE WHEN f.distance_category = 'Medium' THEN 1 ELSE 0 END) AS medium_distance_trips,
    SUM(CASE WHEN f.is_airport_trip THEN 1 ELSE 0 END) AS airport_trips

FROM mart.fact_trip f
INNER JOIN mart.dim_time t ON f.time_key = t.time_key
INNER JOIN mart.dim_date d ON f.date_key = d.date_key
WHERE f.date_key BETWEEN @start_date AND @end_date
GROUP BY
    f.date_key,
    f.time_key,
    t.time_of_day,
    t.is_rush_hour,
    d.is_weekend
