# SQLMesh Models - NYC Yellow Taxi Analytics

This directory contains SQLMesh models for analyzing NYC Yellow Taxi trip data from 2015-2016.

## Architecture Overview

The models are organized in a three-layer architecture:

```
Source Layer (PostgreSQL)
    ↓
Intermediate Layer (Cleaning & Enrichment)
    ↓
Mart Layer (Star Schema: Dimensions + Fact + Analytics)
```

## Layer Structure

### 1. Intermediate Layer (`models/intermediate/`)

Shared transformation logic used across all downstream models.

#### `int_base_yellow_trip.sql`
- **Purpose**: Unions raw data from 2015 and 2016 tables
- **Kind**: INCREMENTAL_BY_TIME_RANGE
- **Time Column**: `tpep_pickup_datetime`

#### `int_cleaned_yellow_trip.sql`
- **Purpose**: Data quality filtering and validation
- **Kind**: INCREMENTAL_BY_TIME_RANGE
- **Filters Applied**:
  - Valid timestamps (pickup < dropoff)
  - Trip distance: 0-100 miles
  - Fare amount: $0-$500
  - Total amount: $0-$1,000
  - Passenger count: 1-6
  - Geographic boundaries: NYC area only
- **Dependencies**: `int_base_yellow_trip`

#### `int_enriched_yellow_trip.sql`
- **Purpose**: Shared calculations and categorizations
- **Kind**: INCREMENTAL_BY_TIME_RANGE
- **Calculated Metrics**:
  - `trip_duration_minutes`: Trip duration in minutes
  - `avg_speed_mph`: Average speed during trip
  - `fare_per_mile`: Fare amount per mile
  - `tip_percentage`: Tip as percentage of fare
  - `revenue_per_minute`: Revenue efficiency metric
- **Categorizations**:
  - `distance_category`: Short/Medium/Long/Very Long
  - `duration_category`: Quick/Normal/Long/Very Long
  - `time_of_day`: Morning/Afternoon/Evening/Night
- **Flags**:
  - `is_rush_hour`: 7-9 AM or 5-7 PM
  - `is_weekend`: Saturday or Sunday
  - `is_airport_trip`: JFK or Newark trips
  - `is_high_tip`: Tip > 20% of fare
- **Dependencies**: `int_cleaned_yellow_trip`

---

### 2. Mart Layer - Dimensions (`models/mart/dim_*.sql`)

Dimension tables with consistent surrogate keys.

#### `dim_date.sql`
- **Primary Key**: `date_key` (DATE)
- **Kind**: FULL
- **Attributes**: year, quarter, month, day, day_of_week, week_of_year, month_name, day_name, is_weekend

#### `dim_time.sql`
- **Primary Key**: `time_key` (INTEGER, 0-23)
- **Kind**: FULL
- **Attributes**: time_of_day, is_rush_hour

#### `dim_vendor.sql`
- **Primary Key**: `vendor_key` (INTEGER)
- **Kind**: FULL
- **Attributes**: vendor_name (Creative Mobile Technologies, VeriFone Inc)

#### `dim_payment_type.sql`
- **Primary Key**: `payment_type_key` (INTEGER)
- **Kind**: FULL
- **Attributes**: payment_type_name (Credit card, Cash, No charge, Dispute, Unknown, Voided trip)

#### `dim_rate_code.sql`
- **Primary Key**: `rate_code_key` (INTEGER)
- **Kind**: FULL
- **Attributes**: rate_code_name (Standard rate, JFK, Newark, Nassau or Westchester, Negotiated fare, Group ride)

---

### 3. Mart Layer - Fact Table (`models/mart/fact_trip.sql`)

Central fact table connecting all dimensions.

#### `fact_trip.sql`
- **Kind**: INCREMENTAL_BY_TIME_RANGE
- **Time Column**: `pickup_datetime`
- **Primary Key**: `trip_key` (MD5 hash)

**Dimension Foreign Keys**:
- `date_key` → `dim_date.date_key`
- `time_key` → `dim_time.time_key`
- `vendor_key` → `dim_vendor.vendor_key`
- `payment_type_key` → `dim_payment_type.payment_type_key`
- `rate_code_key` → `dim_rate_code.rate_code_key`

**Measures**:
- Trip details: passenger_count, trip_distance, trip_duration_minutes
- Location: pickup/dropoff lat/long
- Financial: fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount
- Calculated: fare_per_mile, avg_speed_mph, tip_percentage, revenue_per_minute
- Categories: distance_category, duration_category
- Flags: store_and_fwd_flag, is_rush_hour, is_weekend, is_airport_trip, is_high_tip

---

### 4. Mart Layer - Analytics (`models/mart/analytics_*.sql`)

Pre-aggregated analytics tables for common queries.

#### `analytics_daily_summary.sql`
- **Purpose**: Daily KPIs and trends
- **Grain**: One row per date
- **Metrics**:
  - Trip volume and active vendors
  - Passenger totals and averages
  - Distance and duration aggregates
  - Revenue metrics (fare, tips, total)
  - Payment method distribution
  - Rate code distribution
  - Trip category breakdowns

#### `analytics_hourly_pattern.sql`
- **Purpose**: Hourly demand patterns
- **Grain**: One row per date + hour
- **Metrics**:
  - Trip volume by hour
  - Performance by time of day
  - Rush hour vs non-rush hour analysis
  - Weekend vs weekday patterns
  - Average metrics by hour

#### `analytics_vendor_performance.sql`
- **Purpose**: Vendor comparison and market analysis
- **Grain**: One row per date + vendor
- **Metrics**:
  - Market share percentage
  - Trip volume and passengers
  - Distance and speed metrics
  - Revenue and tips
  - Payment method preferences
  - Trip type distribution

#### `analytics_payment_method.sql`
- **Purpose**: Payment behavior analysis
- **Grain**: One row per date + payment type
- **Metrics**:
  - Payment method share
  - Tip analysis (credit cards typically report tips)
  - Revenue by payment type
  - Trip characteristics by payment method
  - Additional charges breakdown

#### `analytics_distance_cohort.sql`
- **Purpose**: Trip segmentation by distance
- **Grain**: One row per date + distance category
- **Metrics**:
  - Trip distribution by distance bracket
  - Revenue patterns by distance
  - Duration and speed analysis
  - Passenger behavior by distance
  - Time and payment preferences

---

## Star Schema Relationships

```
         dim_date ────────┐
         dim_time ────────┤
         dim_vendor ──────┤
  dim_payment_type ───────┼───── fact_trip ───── analytics_*
     dim_rate_code ───────┘
```

## Key Design Principles

1. **Consistent Surrogate Keys**: All dimension-fact joins use matching key names
2. **Shared Logic**: Common calculations are in the intermediate layer
3. **Incremental Loading**: Most models support time-based incremental processing
4. **Data Quality**: Cleaning happens early in the intermediate layer
5. **Business Logic**: Flags and categories are pre-calculated in enriched layer
6. **Separation of Concerns**: Clear boundaries between raw, cleaned, enriched, and analytical layers

## Usage

All models are incremental by time range, supporting efficient data pipeline execution:

```bash
# Plan changes
sqlmesh plan

# Apply to dev environment
sqlmesh plan dev

# Apply to prod
sqlmesh plan prod
```

## Model Dependencies

```
int_base_yellow_trip
    ↓
int_cleaned_yellow_trip
    ↓
int_enriched_yellow_trip
    ↓
    ├─ dim_date
    ├─ dim_time
    ├─ dim_vendor
    ├─ dim_payment_type
    ├─ dim_rate_code
    └─ fact_trip
        ↓
        ├─ analytics_daily_summary
        ├─ analytics_hourly_pattern
        ├─ analytics_vendor_performance
        ├─ analytics_payment_method
        └─ analytics_distance_cohort
```
