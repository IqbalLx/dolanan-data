MODEL (
  name mart.dim_rate_code,
  kind FULL
);

SELECT DISTINCT
    ratecodeid AS rate_code_key,
    CASE ratecodeid
        WHEN 1 THEN 'Standard rate'
        WHEN 2 THEN 'JFK'
        WHEN 3 THEN 'Newark'
        WHEN 4 THEN 'Nassau or Westchester'
        WHEN 5 THEN 'Negotiated fare'
        WHEN 6 THEN 'Group ride'
        ELSE 'Unknown'
    END AS rate_code_name
FROM intermediate.enriched_yellow_trip
WHERE ratecodeid IS NOT NULL
