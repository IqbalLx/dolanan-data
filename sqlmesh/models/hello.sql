MODEL (
  name staging.hello,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key name
  )
);

SELECT
    'iqbal' as name
