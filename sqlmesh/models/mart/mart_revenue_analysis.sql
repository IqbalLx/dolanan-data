MODEL (
  name nyc_taxi_catalog.mart.mart_revenue_analysis,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column pickup_date,
    batch_size 10000
  ),

  cron '@daily',
  grain (pickup_date, payment_type)
);

-- Revenue analysis by payment type and date
SELECT
    pickup_date,
    payment_type,

    -- Day attributes
    DAY_OF_WEEK(pickup_date) AS day_of_week,
    CASE
        WHEN DAY_OF_WEEK(pickup_date) IN (6, 7) THEN 'weekend'
        ELSE 'weekday'
    END AS day_type,

    -- Volume metrics
    COUNT(*) AS total_trips,
    SUM(passenger_count) AS total_passengers,
    AVG(passenger_count) AS avg_passengers_per_trip,

    -- Revenue breakdown
    SUM(fare_amount) AS total_fare_amount,
    SUM(extra) AS total_extra_charges,
    SUM(mta_tax) AS total_mta_tax,
    SUM(tip_amount) AS total_tip_amount,
    SUM(tolls_amount) AS total_tolls,
    SUM(improvement_surcharge) AS total_surcharge,
    SUM(total_amount) AS total_revenue,

    -- Average revenue metrics
    AVG(fare_amount) AS avg_fare,
    AVG(tip_amount) AS avg_tip,
    AVG(total_amount) AS avg_total_amount,

    -- Revenue per metrics
    SUM(total_amount) / NULLIF(COUNT(*), 0) AS revenue_per_trip,
    SUM(total_amount) / NULLIF(SUM(passenger_count), 0) AS revenue_per_passenger,
    SUM(total_amount) / NULLIF(SUM(trip_distance), 0) AS revenue_per_mile,
    SUM(fare_amount) / NULLIF(SUM(trip_distance), 0) AS fare_per_mile,

    -- Tip analysis
    SUM(tip_amount) / NULLIF(SUM(fare_amount), 0) * 100 AS avg_tip_percentage,
    SUM(CASE WHEN tip_amount > 0 THEN 1 ELSE 0 END) AS trips_with_tips,
    SUM(CASE WHEN tip_amount > 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100 AS tip_rate_percentage,

    -- Distance metrics
    SUM(trip_distance) AS total_distance,
    AVG(trip_distance) AS avg_distance,

    -- Revenue by time category
    SUM(CASE WHEN time_of_day_category = 'morning_rush' THEN total_amount ELSE 0 END) AS morning_rush_revenue,
    SUM(CASE WHEN time_of_day_category = 'evening_rush' THEN total_amount ELSE 0 END) AS evening_rush_revenue,
    SUM(CASE WHEN time_of_day_category = 'late_night' THEN total_amount ELSE 0 END) AS late_night_revenue,
    SUM(CASE WHEN time_of_day_category = 'off_peak' THEN total_amount ELSE 0 END) AS off_peak_revenue,

    -- Revenue by trip distance category
    SUM(CASE WHEN trip_distance_category = 'short' THEN total_amount ELSE 0 END) AS short_trip_revenue,
    SUM(CASE WHEN trip_distance_category = 'medium' THEN total_amount ELSE 0 END) AS medium_trip_revenue,
    SUM(CASE WHEN trip_distance_category = 'long' THEN total_amount ELSE 0 END) AS long_trip_revenue,
    SUM(CASE WHEN trip_distance_category = 'very_long' THEN total_amount ELSE 0 END) AS very_long_trip_revenue,

    -- Revenue composition percentages
    SUM(fare_amount) / NULLIF(SUM(total_amount), 0) * 100 AS fare_percentage_of_total,
    SUM(tip_amount) / NULLIF(SUM(total_amount), 0) * 100 AS tip_percentage_of_total,
    SUM(tolls_amount) / NULLIF(SUM(total_amount), 0) * 100 AS tolls_percentage_of_total,
    SUM(extra) / NULLIF(SUM(total_amount), 0) * 100 AS extra_percentage_of_total,

    -- High value trip indicators
    SUM(CASE WHEN total_amount > 50 THEN 1 ELSE 0 END) AS high_value_trips,
    SUM(CASE WHEN total_amount > 50 THEN total_amount ELSE 0 END) AS high_value_trip_revenue,

    -- Payment type description
    CASE payment_type
        WHEN 1 THEN 'Credit Card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No Charge'
        WHEN 4 THEN 'Dispute'
        WHEN 5 THEN 'Unknown'
        WHEN 6 THEN 'Voided Trip'
        ELSE 'Other'
    END AS payment_type_description

FROM nyc_taxi_catalog.intermediate.int_taxi_trips_cleaned
WHERE pickup_date BETWEEN @start_date AND @end_date
GROUP BY pickup_date, payment_type
