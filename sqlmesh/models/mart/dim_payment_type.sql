MODEL (
  name mart.dim_payment_type,
  kind FULL
);

SELECT DISTINCT
    payment_type AS payment_type_key,
    CASE payment_type
        WHEN 1 THEN 'Credit card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No charge'
        WHEN 4 THEN 'Dispute'
        WHEN 5 THEN 'Unknown'
        WHEN 6 THEN 'Voided trip'
        ELSE 'Unknown'
    END AS payment_type_name
FROM intermediate.enriched_yellow_trip
WHERE payment_type IS NOT NULL
