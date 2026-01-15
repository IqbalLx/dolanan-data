MODEL (
  name nyc_taxi_catalog.intermediate.int_geographic_heatmap,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column pickup_date,
    batch_size 10000
  ),

  cron '@daily',
  grain (pickup_date, pickup_zone, hour_of_day)
);

-- Geographic heatmap data for visualizing demand patterns across NYC
SELECT
    pickup_date,

    -- Create pickup zone by binning coordinates (approx 0.01 degree bins ~ 1km)
    CONCAT(
        CAST(FLOOR(pickup_latitude / 0.01) * 0.01 AS VARCHAR),
        ',',
        CAST(FLOOR(pickup_longitude / 0.01) * 0.01 AS VARCHAR)
    ) AS pickup_zone,

    pickup_hour AS hour_of_day,

    -- Zone center coordinates
    FLOOR(pickup_latitude / 0.01) * 0.01 + 0.005 AS zone_center_latitude,
    FLOOR(pickup_longitude / 0.01) * 0.01 + 0.005 AS zone_center_longitude,

    -- Trip density metrics
    COUNT(*) AS trip_count,
    COUNT(DISTINCT vendor_id) AS vendor_count,
    SUM(passenger_count) AS total_passengers,

    -- Revenue density
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_revenue_per_trip,

    -- Distance metrics
    AVG(trip_distance) AS avg_trip_distance,

    -- Duration metrics
    AVG(trip_duration_minutes) AS avg_trip_duration,

    -- Tip metrics
    AVG(tip_percentage) AS avg_tip_percentage,

    -- Time categorization
    time_of_day_category,
    day_type,

    -- Payment distribution
    SUM(CASE WHEN payment_type = 1 THEN 1 ELSE 0 END) AS credit_card_trips,
    SUM(CASE WHEN payment_type = 2 THEN 1 ELSE 0 END) AS cash_trips,

    -- Activity intensity score (normalized)
    COUNT(*) * 1.0 / NULLIF(
        SUM(COUNT(*)) OVER (PARTITION BY pickup_date), 0
    ) * 100 AS zone_activity_percentage

FROM nyc_taxi_catalog.intermediate.int_taxi_trips_cleaned
WHERE pickup_date BETWEEN @start_date AND @end_date
GROUP BY
    pickup_date,
    CONCAT(
        CAST(FLOOR(pickup_latitude / 0.01) * 0.01 AS VARCHAR),
        ',',
        CAST(FLOOR(pickup_longitude / 0.01) * 0.01 AS VARCHAR)
    ),
    pickup_hour,
    FLOOR(pickup_latitude / 0.01) * 0.01,
    FLOOR(pickup_longitude / 0.01) * 0.01,
    time_of_day_category,
    day_type
