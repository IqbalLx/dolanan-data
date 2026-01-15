MODEL (
  name nyc_taxi_catalog.mart.mart_geographic_analysis,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column pickup_date,
    batch_size 10000
  ),

  cron '@daily',
  grain (pickup_date, pickup_zone)
);

-- Geographic analysis of taxi demand with heatmap data
SELECT
    pickup_date,
    pickup_zone,

    -- Zone coordinates
    MAX(zone_center_latitude) AS zone_center_latitude,
    MAX(zone_center_longitude) AS zone_center_longitude,

    -- Day attributes
    DAY_OF_WEEK(pickup_date) AS day_of_week,
    CASE
        WHEN DAY_OF_WEEK(pickup_date) IN (6, 7) THEN 'weekend'
        ELSE 'weekday'
    END AS day_type,

    -- Overall trip metrics
    SUM(trip_count) AS total_trips,
    SUM(total_passengers) AS total_passengers,
    AVG(avg_trip_distance) AS avg_trip_distance_miles,
    AVG(avg_trip_duration) AS avg_trip_duration_minutes,

    -- Revenue metrics
    SUM(total_revenue) AS total_revenue,
    AVG(avg_revenue_per_trip) AS avg_revenue_per_trip,
    SUM(total_revenue) / NULLIF(SUM(trip_count), 0) AS revenue_per_trip,

    -- Tip analysis
    AVG(avg_tip_percentage) AS avg_tip_percentage,

    -- Payment distribution
    SUM(credit_card_trips) AS credit_card_trips,
    SUM(cash_trips) AS cash_trips,
    SUM(credit_card_trips) / NULLIF(SUM(trip_count), 0) * 100 AS credit_card_percentage,

    -- Time distribution
    SUM(CASE WHEN time_of_day_category = 'morning_rush' THEN trip_count ELSE 0 END) AS morning_rush_trips,
    SUM(CASE WHEN time_of_day_category = 'evening_rush' THEN trip_count ELSE 0 END) AS evening_rush_trips,
    SUM(CASE WHEN time_of_day_category = 'late_night' THEN trip_count ELSE 0 END) AS late_night_trips,
    SUM(CASE WHEN time_of_day_category = 'off_peak' THEN trip_count ELSE 0 END) AS off_peak_trips,

    -- Hourly distribution
    SUM(CASE WHEN hour_of_day BETWEEN 0 AND 5 THEN trip_count ELSE 0 END) AS trips_0_5am,
    SUM(CASE WHEN hour_of_day BETWEEN 6 AND 11 THEN trip_count ELSE 0 END) AS trips_6_11am,
    SUM(CASE WHEN hour_of_day BETWEEN 12 AND 17 THEN trip_count ELSE 0 END) AS trips_12_5pm,
    SUM(CASE WHEN hour_of_day BETWEEN 18 AND 23 THEN trip_count ELSE 0 END) AS trips_6_11pm,

    -- Peak hour metrics
    MAX(trip_count) AS peak_hour_trips,
    MIN(trip_count) AS min_hour_trips,

    -- Activity intensity
    AVG(zone_activity_percentage) AS avg_zone_activity_percentage,

    -- Zone ranking by activity
    RANK() OVER (
        PARTITION BY pickup_date
        ORDER BY SUM(trip_count) DESC
    ) AS zone_rank_by_trips,

    -- Zone ranking by revenue
    RANK() OVER (
        PARTITION BY pickup_date
        ORDER BY SUM(total_revenue) DESC
    ) AS zone_rank_by_revenue,

    -- Hot zone indicator (top 20% by trip volume)
    CASE
        WHEN SUM(trip_count) >= PERCENTILE_CONT(0.80) WITHIN GROUP (ORDER BY SUM(trip_count)) OVER (
            PARTITION BY pickup_date
        ) THEN true
        ELSE false
    END AS is_hot_zone,

    -- High revenue zone indicator (top 20% by revenue)
    CASE
        WHEN SUM(total_revenue) >= PERCENTILE_CONT(0.80) WITHIN GROUP (ORDER BY SUM(total_revenue)) OVER (
            PARTITION BY pickup_date
        ) THEN true
        ELSE false
    END AS is_high_revenue_zone,

    -- Efficiency metrics
    SUM(total_revenue) / NULLIF(SUM(trip_count), 0) AS revenue_efficiency,
    AVG(avg_trip_distance) / NULLIF(AVG(avg_trip_duration), 0) * 60 AS avg_speed_mph,

    -- Vendor diversity
    AVG(vendor_count) AS avg_vendor_count

FROM nyc_taxi_catalog.intermediate.int_geographic_heatmap
WHERE pickup_date BETWEEN @start_date AND @end_date
GROUP BY pickup_date, pickup_zone
