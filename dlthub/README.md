# NYC Taxi Data Pipeline - dlt-iceberg

This directory contains a data pipeline built with [dlt (data load tool)](https://dlthub.com) and [dlt-iceberg](https://github.com/dlt-hub/dlt-iceberg) to load NYC Yellow Taxi trip data from MinIO S3 into Apache Iceberg tables managed by Apache Polaris, queryable via Trino.

## Overview

The pipeline performs **Extract-Load (EL)** operations without transformations:

1. **Extract**: Reads CSV files from MinIO S3 bucket (`s3://data`)
2. **Load**: Writes data to Iceberg tables in Polaris catalog
3. **Query**: Data becomes immediately queryable in Trino

```
┌─────────────┐
│   MinIO S3  │
│ (CSV files) │
└──────┬──────┘
       │
       │ Extract (dlt)
       ▼
┌─────────────────┐
│  dlt Pipeline   │
│  (nyc_taxi_     │
│   pipeline.py)  │
└──────┬──────────┘
       │
       │ Load (dlt-iceberg)
       ▼
┌──────────────────┐         ┌─────────────┐
│ Apache Polaris   │ ◄─────► │    Trino    │
│ (Iceberg Catalog)│         │ Query Engine│
└──────────────────┘         └─────────────┘
```

## Architecture

### Components

- **dlt**: Data loading library that handles extraction and normalization
- **dlt-iceberg**: dlt destination for Apache Iceberg tables
- **MinIO**: S3-compatible object storage containing source CSV files
- **Apache Polaris**: REST catalog service for Iceberg metadata
- **Trino**: Distributed query engine for data analysis

### Data Flow

1. **CSV Files** → Located in MinIO at `s3://data/yellow_tripdata_*.csv`
2. **dlt Extraction** → Reads CSV files using DuckDB for efficient chunked processing
3. **dlt-iceberg Load** → Writes to Iceberg tables via Polaris REST API
4. **Iceberg Storage** → Parquet files stored in MinIO at `s3://warehouse/`
5. **Query Access** → Trino queries via `nyc_taxi_catalog.raw.yellow_tripdata`

## Prerequisites

1. **Docker services running**:
   ```bash
   docker-compose up -d
   docker-compose ps  # Verify all services are healthy
   ```

2. **CSV files uploaded to MinIO**:
   ```bash
   # Upload CSV files from raw_data/
   ./scripts/upload_csv.sh
   
   # Or manually verify files exist
   aws --endpoint-url http://localhost:9000 s3 ls s3://data/
   ```

3. **Python dependencies installed**:
   ```bash
   # From project root
   uv sync
   # or
   pip install -e .
   ```

## Configuration

### DLT Configuration (`.dlt/config.toml`)

```toml
[sources.filesystem]
bucket_url = "s3://data"

[sources.filesystem.credentials]
aws_access_key_id = "minio_root"
aws_secret_access_key = "m1n1opwd"
endpoint_url = "http://localhost:9000"
region_name = "us-west-2"

[destination.iceberg_rest]
catalog_uri = "http://localhost:8181/api/catalog"
oauth2_server_uri = "http://localhost:8181/api/catalog/v1/oauth/tokens"
credential = "root:s3cr3t"
warehouse = "nyc_taxi_catalog"
namespace = "raw"
```

### Service Endpoints

| Service | Endpoint | Credentials |
|---------|----------|-------------|
| MinIO API | http://localhost:9000 | minio_root / m1n1opwd |
| MinIO Console | http://localhost:9001 | minio_root / m1n1opwd |
| Polaris API | http://localhost:8181 | root / s3cr3t |
| Trino UI | http://localhost:8080 | trino / (no password) |

## Usage

### Basic Pipeline Execution

Run the pipeline from the `dlthub/` directory:

```bash
cd dlthub
python nyc_taxi_pipeline.py
```

### What Happens

1. **Service Verification**: Checks MinIO and Polaris connectivity
2. **CSV Reading**: Scans `s3://data/yellow_tripdata_*.csv` files
3. **Chunked Processing**: Processes CSV in 50,000 row chunks for memory efficiency
4. **Iceberg Load**: Writes data as Parquet files to `s3://warehouse/`
5. **Catalog Update**: Registers table metadata in Polaris
6. **Completion**: Data is queryable in Trino

### Expected Output

```
================================================================================
NYC Taxi Data Pipeline - CSV to Iceberg via Polaris
================================================================================

Configuration:
  Source: s3://data/yellow_tripdata_*.csv
  Catalog: http://localhost:8181/api/catalog
  Namespace: raw
  Warehouse: nyc_taxi_catalog
================================================================================

🔍 Verifying service connectivity...
  ✓ MinIO is accessible
  ✓ Polaris is accessible
  ✓ Polaris authentication successful

✅ All services are accessible!

📁 Reading CSV files from MinIO...

🚀 Starting data load to Iceberg tables...
   This may take several minutes for large CSV files...

================================================================================
Load Summary
================================================================================
Pipeline name: nyc_taxi_pipeline
Destination: Iceberg REST (Apache Polaris)
Dataset: raw

✅ Pipeline completed successfully!
================================================================================

📊 Query your data in Trino:
   SELECT * FROM nyc_taxi_catalog.raw.yellow_tripdata LIMIT 10;

💡 Access Trino UI: http://localhost:8080
================================================================================
```

## Pipeline Features

### Standard Load (`load_nyc_taxi_data()`)

- **Write Disposition**: Append (adds new records)
- **Processing**: Chunked CSV reading (50k rows per chunk)
- **Memory Efficient**: Streams data without loading entire files
- **Target Table**: `nyc_taxi_catalog.raw.yellow_tripdata`

### Incremental Load (`load_incremental_taxi_data()`)

- **Tracking**: Only processes new/modified files based on modification date
- **State Management**: dlt tracks which files have been loaded
- **Idempotent**: Safe to run multiple times
- **Use Case**: Ongoing data ingestion pipelines

## Querying Data

### Via Trino CLI

```bash
# Connect to Trino
docker exec -it data-platform-poc-trino-1 trino

# Query the data
SELECT COUNT(*) FROM nyc_taxi_catalog.raw.yellow_tripdata;

SELECT 
    tpep_pickup_datetime,
    passenger_count,
    trip_distance,
    total_amount
FROM nyc_taxi_catalog.raw.yellow_tripdata
LIMIT 10;
```

### Via Python

```python
from trino.dbapi import connect

conn = connect(
    host='localhost',
    port=8080,
    user='trino',
    catalog='nyc_taxi_catalog',
    schema='raw'
)

cur = conn.cursor()
cur.execute("SELECT COUNT(*) FROM yellow_tripdata")
print(cur.fetchone())
```

### Via DuckDB (with Iceberg extension)

```sql
LOAD iceberg;

-- Configure S3 credentials
SET s3_access_key_id='minio_root';
SET s3_secret_access_key='m1n1opwd';
SET s3_endpoint='localhost:9000';
SET s3_use_ssl=false;
SET s3_region='us-west-2';

-- Attach catalog
ATTACH 'nyc_taxi_catalog' AS nyc_taxi (
    TYPE ICEBERG,
    ENDPOINT 'http://localhost:8181/api/catalog',
    TOKEN '<get-token-from-polaris>'
);

-- Query
SELECT * FROM nyc_taxi.raw.yellow_tripdata LIMIT 10;
```

## Data Schema

The NYC Yellow Taxi dataset includes columns like:

- `VendorID`: Provider identifier
- `tpep_pickup_datetime`: Pickup timestamp
- `tpep_dropoff_datetime`: Dropoff timestamp
- `passenger_count`: Number of passengers
- `trip_distance`: Trip distance in miles
- `pickup_longitude`, `pickup_latitude`: Pickup coordinates
- `dropoff_longitude`, `dropoff_latitude`: Dropoff coordinates
- `fare_amount`: Base fare
- `tip_amount`: Tip amount
- `total_amount`: Total amount charged

*Schema may vary by year/dataset version*

## Troubleshooting

### MinIO Connection Error

```
✗ MinIO is not accessible
```

**Solution**:
```bash
# Check MinIO is running
docker-compose ps minio

# Start MinIO if needed
docker-compose up -d minio

# Verify endpoint
curl http://localhost:9000/minio/health/live
```

### Polaris Authentication Failed

```
✗ Polaris authentication failed
```

**Solution**:
```bash
# Check Polaris is healthy
docker-compose ps polaris

# View Polaris logs
docker-compose logs polaris

# Verify credentials in docker-compose.yml
grep POLARIS_BOOTSTRAP_CREDENTIALS docker-compose.yml
```

### No CSV Files Found

```
No files matching pattern in s3://data/
```

**Solution**:
```bash
# Upload CSV files
./scripts/upload_csv.sh

# Verify files exist
aws --endpoint-url http://localhost:9000 s3 ls s3://data/
```

### Import Error: dlt_iceberg

```
ModuleNotFoundError: No module named 'dlt_iceberg'
```

**Solution**:
```bash
# Install dependencies
uv sync

# Or manually install
pip install dlt-iceberg dlt[filesystem]
```

### Catalog Not Found

```
Catalog 'nyc_taxi_catalog' does not exist
```

**Solution**:
```bash
# Check catalog creation
docker-compose logs polaris-setup

# Recreate catalog
docker-compose up -d polaris-setup
```

## Pipeline State

dlt maintains pipeline state in `.dlt/` directory:

- **Pipeline metadata**: Tracks loaded files, schemas, etc.
- **Incremental state**: Remembers last processed modification date
- **Local storage**: State is stored locally in SQLite databases

To reset state (reprocess all files):
```bash
rm -rf .dlt/pipeline_state/
```

## Performance Tips

1. **Chunk Size**: Adjust `chunk_size` parameter based on available memory
   - Larger chunks = faster but more memory
   - Smaller chunks = slower but less memory

2. **Parallel Processing**: dlt supports parallel extraction (advanced)

3. **File Filtering**: Use more specific glob patterns to process fewer files
   ```python
   file_glob="yellow_tripdata_2016-*.csv"  # Only 2016 data
   ```

4. **Incremental Loading**: Use incremental mode to avoid reprocessing
   ```python
   load_incremental_taxi_data()  # Only new files
   ```

## Integration with SQLMesh

After loading data with dlt, transform it with SQLMesh:

```sql
-- sqlmesh/models/staging/yellow_taxi.sql
MODEL (
  name staging.yellow_taxi,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column tpep_pickup_datetime
  )
);

SELECT
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  passenger_count,
  trip_distance,
  fare_amount,
  tip_amount,
  total_amount
FROM nyc_taxi_catalog.raw.yellow_tripdata
WHERE tpep_pickup_datetime BETWEEN @start_date AND @end_date
```

## References

- [dlt Documentation](https://dlthub.com/docs)
- [dlt-iceberg GitHub](https://github.com/dlt-hub/dlt-iceberg)
- [Apache Iceberg](https://iceberg.apache.org/)
- [Apache Polaris](https://polaris.apache.org/)
- [Trino Iceberg Connector](https://trino.io/docs/current/connector/iceberg.html)

## Next Steps

1. **Load Data**: Run the pipeline to load CSV files into Iceberg
2. **Verify in Trino**: Query the data to ensure it loaded correctly
3. **Transform with SQLMesh**: Build transformation models on top of raw data
4. **Schedule**: Set up automated pipeline runs (e.g., Airflow, cron)
5. **Monitor**: Track pipeline execution and data quality

---

**Questions?** Check the main project README or explore the code in `nyc_taxi_pipeline.py`
