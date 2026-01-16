MODEL (
  name mart.dim_vendor,
  kind FULL
);

SELECT DISTINCT
    vendorid AS vendor_key,
    CASE vendorid
        WHEN 1 THEN 'Creative Mobile Technologies'
        WHEN 2 THEN 'VeriFone Inc'
        ELSE 'Unknown'
    END AS vendor_name
FROM intermediate.enriched_yellow_trip
WHERE vendorid IS NOT NULL
