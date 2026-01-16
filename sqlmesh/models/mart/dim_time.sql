MODEL (
  name mart.dim_time,
  kind FULL
);

WITH time_spine AS (
    SELECT DISTINCT
        pickup_hour AS hour
    FROM intermediate.enriched_yellow_trip
)

SELECT
    hour AS time_key,
    CASE
        WHEN hour BETWEEN 6 AND 11 THEN 'Morning'
        WHEN hour BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN hour BETWEEN 18 AND 21 THEN 'Evening'
        ELSE 'Night'
    END AS time_of_day,
    CASE
        WHEN hour BETWEEN 7 AND 9 THEN TRUE
        WHEN hour BETWEEN 17 AND 19 THEN TRUE
        ELSE FALSE
    END AS is_rush_hour
FROM time_spine
