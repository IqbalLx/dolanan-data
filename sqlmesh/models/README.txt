NYC TAXI TRIP DATA - SQLMESH MODELS
====================================

This directory contains SQLMesh models for analyzing NYC Yellow Taxi trip data.
The models are organized into two layers: intermediate and mart.

DATA SOURCE
-----------
Raw data is loaded into: nyc_taxi_catalog.raw.yellow_tripdata

Available columns:
- vendor_id: Taxi vendor identifier
- tpep_pickup_datetime: Pickup timestamp
- tpep_dropoff_datetime: Dropoff timestamp
- passenger_count: Number of passengers
- trip_distance: Trip distance in miles
- pickup_longitude, pickup_latitude: Pickup coordinates
- dropoff_longitude, dropoff_latitude: Dropoff coordinates
- rate_code_id: Rate code for the trip
- store_and_fwd_flag: Store and forward flag
- payment_type: Payment method (1=Credit Card, 2=Cash, etc.)
- fare_amount: Base fare amount
- extra: Extra charges
- mta_tax: MTA tax
- tip_amount: Tip amount
- tolls_amount: Tolls paid
- improvement_surcharge: Improvement surcharge
- total_amount: Total trip amount

GEOGRAPHIC ZONES
----------------
Since the raw data uses lat/lon coordinates instead of location IDs, all models
create geographic zones by binning coordinates into ~1km grid cells (0.01 degree bins).

Zone format: "latitude,longitude" (e.g., "40.75,-73.98")

INTERMEDIATE MODELS
-------------------

1. int_taxi_trips_cleaned.sql
   - Cleaned and validated trip-level data
   - Data quality filters applied
   - Calculated fields: trip_duration_minutes, tip_percentage
   - Trip categorizations: distance_category, time_of_day_category, day_type
   - Grain: vendor_id, pickup_datetime, dropoff_datetime

2. int_daily_trip_metrics.sql
   - Daily aggregations by pickup zone
   - Trip counts, passenger metrics, distance/duration stats
   - Revenue aggregations by zone
   - Time and payment category breakdowns
   - Grain: pickup_date, pickup_zone

3. int_hourly_demand.sql
   - Hourly demand patterns by pickup zone
   - Trip counts and passenger metrics per hour
   - Revenue and tip analysis by hour
   - Time categorization
   - Grain: pickup_datetime_hour, pickup_zone

4. int_location_pairs.sql
   - Route analysis (pickup-dropoff zone pairs)
   - Distance and duration statistics per route
   - Fare and tip metrics by route
   - Time distribution analysis
   - Grain: pickup_date, pickup_zone, dropoff_zone

5. int_geographic_heatmap.sql
   - Geographic heatmap data for visualization
   - Trip density by zone and hour
   - Zone activity percentages
   - Revenue density metrics
   - Grain: pickup_date, pickup_zone, hour_of_day

MART MODELS
-----------

1. mart_daily_summary.sql
   - City-wide daily KPIs and performance metrics
   - Trip volume, revenue, and efficiency trends
   - Payment method distribution
   - Trip category breakdowns
   - Use case: Executive dashboards, daily reporting
   - Grain: pickup_date

2. mart_location_performance.sql
   - Performance metrics by pickup zone
   - Revenue efficiency and utilization scores
   - Trip category distributions
   - Time-based activity patterns
   - Use case: Zone-level performance analysis, resource allocation
   - Grain: pickup_date, pickup_zone

3. mart_hourly_demand_patterns.sql
   - Hourly demand patterns with comparisons
   - Demand intensity scores
   - Peak hour identification
   - Revenue per hour metrics
   - Use case: Demand forecasting, driver scheduling
   - Grain: pickup_datetime_hour, pickup_zone

4. mart_popular_routes.sql
   - Popular route analysis and ranking
   - Route consistency scores
   - Premium and high-volume route identification
   - Revenue per route metrics
   - Use case: Route optimization, pricing strategies
   - Grain: pickup_date, pickup_zone, dropoff_zone

5. mart_revenue_analysis.sql
   - Revenue breakdown by payment type
   - Tip analysis and tip rates
   - Revenue composition percentages
   - High-value trip identification
   - Use case: Revenue optimization, payment analysis
   - Grain: pickup_date, payment_type

6. mart_vendor_performance.sql
   - Vendor comparison and performance tracking
   - Market share analysis
   - Service quality indicators
   - Vendor rankings
   - Use case: Vendor management, competitive analysis
   - Grain: pickup_date, vendor_id

7. mart_time_trends.sql
   - Time-based trends with rolling averages
   - Day-over-day, week-over-week, month-over-month growth
   - Peak performance identification
   - Temporal pattern analysis
   - Use case: Trend analysis, forecasting, anomaly detection
   - Grain: pickup_date

8. mart_geographic_analysis.sql
   - Geographic analysis with heatmap data
   - Zone ranking by activity and revenue
   - Hot zone and high-revenue zone identification
   - Hourly distribution by zone
   - Use case: Geographic visualization, heat mapping, zone analysis
   - Grain: pickup_date, pickup_zone

COMMON ANALYTICAL PATTERNS
---------------------------

Trip Distance Categories:
- short: <= 1 mile
- medium: 1-5 miles
- long: 5-10 miles
- very_long: > 10 miles

Time of Day Categories:
- morning_rush: 6am-9am
- evening_rush: 5pm-8pm
- late_night: 10pm-5am
- off_peak: other times

Day Types:
- weekday: Monday-Friday
- weekend: Saturday-Sunday

DATA QUALITY FILTERS
--------------------
All intermediate models apply these filters:
- Valid pickup/dropoff timestamps
- Dropoff after pickup
- Passenger count: 1-8
- Trip distance: 0-100 miles
- Fare amount: $0-$500
- Total amount: $0-$1000
- Coordinates within NYC bounds (approx -74.05 to -73.75 lon, 40.6 to 40.9 lat)

DEPLOYMENT
----------
All models are configured with:
- Incremental materialization by time range
- Daily cron schedule (@daily)
- Start date: 2009-01-01
- Trino SQL dialect
- Gateway: warehouse (Trino on localhost:8080)
- Catalog: nyc_taxi_catalog

To deploy:
1. sqlmesh plan
2. Review changes
3. sqlmesh run

To backfill:
sqlmesh run --start-date YYYY-MM-DD --end-date YYYY-MM-DD
