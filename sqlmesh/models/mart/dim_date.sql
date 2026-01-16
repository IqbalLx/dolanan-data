MODEL (
  name mart.dim_date,
  kind FULL
);

WITH date_spine AS (
    SELECT DISTINCT
        pickup_date AS date_day
    FROM intermediate.enriched_yellow_trip
)

SELECT
    date_day AS date_key,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(QUARTER FROM date_day) AS quarter,
    EXTRACT(MONTH FROM date_day) AS month,
    EXTRACT(DAY FROM date_day) AS day,
    EXTRACT(DOW FROM date_day) AS day_of_week,
    EXTRACT(DOY FROM date_day) AS day_of_year,
    EXTRACT(WEEK FROM date_day) AS week_of_year,
    TO_CHAR(date_day, 'Month') AS month_name,
    TO_CHAR(date_day, 'Day') AS day_name,
    CASE
        WHEN EXTRACT(DOW FROM date_day) IN (0, 6) THEN TRUE
        ELSE FALSE
    END AS is_weekend
FROM date_spine
