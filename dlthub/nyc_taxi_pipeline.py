#!/usr/bin/env python3
"""
NYC Taxi Data Pipeline using dlt-iceberg

This pipeline loads NYC taxi trip data from CSV files stored in MinIO S3
into Apache Iceberg tables via Apache Polaris catalog, queryable through Trino.

The pipeline performs Extract-Load (EL) operations without transformations,
reading CSV files from the MinIO 'data' bucket and writing them as Iceberg tables.

Configuration:
- MinIO: localhost:9000 (s3://data bucket)
- Polaris: localhost:8181 (nyc_taxi_catalog)
- Namespace: raw (creates raw.yellow_tripdata table)

Usage:
    python nyc_taxi_pipeline.py
"""

import os

import dlt
from dlt.sources.filesystem import read_csv_duckdb


def load_incremental_taxi_data():
    """
    Load NYC Taxi data incrementally based on file modification time.

    This version tracks which files have been loaded and only processes new ones,
    useful for ongoing data ingestion pipelines.
    """
    from dlt_iceberg import iceberg_rest

    print("=" * 80)
    print("NYC Taxi Data Pipeline - Incremental Load")
    print("=" * 80)

    # Configuration
    s3_bucket_url = "s3://data"
    csv_glob_pattern = "yellow_tripdata_*.csv"

    # Set S3 credentials for MinIO
    os.environ["AWS_ACCESS_KEY_ID"] = "minio_root"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "m1n1opwd"
    os.environ["AWS_ENDPOINT_URL"] = "http://localhost:9000"
    os.environ["AWS_REGION"] = "us-west-2"

    # Create pipeline
    pipeline = dlt.pipeline(
        pipeline_name="nyc_taxi_incremental",
        destination=iceberg_rest(
            catalog_uri="http://localhost:8181/api/catalog",
            namespace="raw",
            warehouse="nyc_taxi_catalog",
            credential="root:s3cr3t",
            oauth2_server_uri="http://localhost:8181/api/catalog/v1/oauth/tokens",
        ),
        dataset_name="raw",
    )

    print("\n📁 Reading new CSV files from MinIO (incremental)...")

    # Read CSV files incrementally based on modification date
    from dlt.sources.filesystem import filesystem

    new_files = filesystem(
        bucket_url=s3_bucket_url,
        file_glob=csv_glob_pattern,
    )

    # Track files by modification date - only load new/modified files
    new_files.apply_hints(incremental=dlt.sources.incremental("modification_date"))

    # Pipe files to CSV reader
    taxi_data = new_files | read_csv_duckdb(
        chunk_size=1000,
        header=True,
        use_pyarrow=True,
    )

    taxi_data.apply_hints(write_disposition="append")

    print("\n🚀 Loading new/modified files to Iceberg...")

    # Run the pipeline
    load_info = pipeline.run(
        taxi_data.with_name("yellow_tripdata"),
    )

    print("\n" + "=" * 80)
    print("Incremental Load Summary")
    print("=" * 80)
    print(load_info)
    print("=" * 80)

    return load_info


def verify_connection():
    """
    Verify connectivity to MinIO and Polaris before running the pipeline.

    Returns:
        bool: True if all services are accessible, False otherwise
    """
    import requests

    print("\n🔍 Verifying service connectivity...")

    # Check MinIO
    try:
        response = requests.get("http://localhost:9000/minio/health/live", timeout=5)
        if response.status_code == 200:
            print("  ✓ MinIO is accessible")
        else:
            print("  ✗ MinIO health check failed")
            return False
    except Exception as e:
        print(f"  ✗ MinIO is not accessible: {e}")
        return False

    # Check Polaris
    try:
        response = requests.get(
            "http://localhost:8181/api/catalog/v1/config", timeout=5
        )
        if response.status_code in [200, 401]:  # 401 is ok, means auth is working
            print("  ✓ Polaris is accessible")
        else:
            print("  ✗ Polaris health check failed")
            return False
    except Exception as e:
        print(f"  ✗ Polaris is not accessible: {e}")
        return False

    # Check if we can get OAuth token
    try:
        response = requests.post(
            "http://localhost:8181/api/catalog/v1/oauth/tokens",
            auth=("root", "s3cr3t"),
            headers={"Polaris-Realm": "POLARIS"},
            data={
                "grant_type": "client_credentials",
                "scope": "PRINCIPAL_ROLE:ALL",
            },
            timeout=5,
        )
        if response.status_code == 200:
            print("  ✓ Polaris authentication successful")
            return True
        else:
            print(f"  ✗ Polaris authentication failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"  ✗ Polaris authentication error: {e}")
        return False


if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("NYC Taxi Data Pipeline - dlt-iceberg")
    print("=" * 80)

    # Verify connectivity
    if not verify_connection():
        print("\n❌ Service connectivity check failed!")
        print("\nPlease ensure Docker services are running:")
        print("  docker-compose up -d")
        print("\nThen verify services are healthy:")
        print("  docker-compose ps")
        exit(1)

    print("\n✅ All services are accessible!\n")

    # Run the main pipeline
    try:
        load_info = load_incremental_taxi_data()
        print("\n✅ Pipeline execution completed successfully!")

    except Exception as e:
        print("\n" + "=" * 80)
        print("❌ Pipeline execution failed!")
        print("=" * 80)
        print(f"Error: {e}")
        print("\nTroubleshooting:")
        print("  1. Verify Docker services: docker-compose ps")
        print(
            "  2. Check MinIO has CSV files: aws --endpoint-url http://localhost:9000 s3 ls s3://data/"
        )
        print("  3. Verify Polaris catalog exists: docker-compose logs polaris-setup")
        print("  4. Check logs: docker-compose logs polaris minio trino")
        raise
