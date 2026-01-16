#!/usr/bin/env python3
"""
Upload CSV files from raw_data/ directory to MinIO S3.

This script uploads all CSV files from the local raw_data/ directory
to the s3://data bucket in MinIO.

Usage:
    python scripts/upload_csv_to_minio.py

Environment Variables (optional):
    MINIO_ENDPOINT: MinIO endpoint (default: localhost:9000)
    MINIO_ACCESS_KEY: MinIO access key (default: minio_root)
    MINIO_SECRET_KEY: MinIO secret key (default: m1n1opwd)
    MINIO_BUCKET: Target bucket name (default: data)
    MINIO_USE_SSL: Use SSL for connection (default: false)
"""

import os
import sys
from pathlib import Path
from typing import List

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError


class MinIOUploader:
    """Handler for uploading files to MinIO S3."""

    def __init__(
        self,
        endpoint: str = "localhost:9000",
        access_key: str = "minio_root",
        secret_key: str = "m1n1opwd",
        use_ssl: bool = False,
    ):
        """
        Initialize MinIO S3 client.

        Args:
            endpoint: MinIO server endpoint
            access_key: MinIO access key
            secret_key: MinIO secret key
            use_ssl: Whether to use SSL/TLS
        """
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.use_ssl = use_ssl

        # Initialize S3 client
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=f"http{'s' if use_ssl else ''}://{endpoint}",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version="s3v4"),
            region_name="us-west-2",
        )

    def ensure_bucket_exists(self, bucket_name: str) -> bool:
        """
        Ensure the target bucket exists, create if it doesn't.

        Args:
            bucket_name: Name of the bucket

        Returns:
            True if bucket exists or was created successfully
        """
        try:
            self.s3_client.head_bucket(Bucket=bucket_name)
            print(f"✓ Bucket '{bucket_name}' exists")
            return True
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            if error_code == "404":
                # Bucket doesn't exist, create it
                try:
                    self.s3_client.create_bucket(Bucket=bucket_name)
                    print(f"✓ Created bucket '{bucket_name}'")
                    return True
                except ClientError as create_error:
                    print(f"✗ Failed to create bucket '{bucket_name}': {create_error}")
                    return False
            else:
                print(f"✗ Error checking bucket '{bucket_name}': {e}")
                return False

    def upload_file(
        self, file_path: Path, bucket_name: str, s3_key: str = None
    ) -> bool:
        """
        Upload a single file to MinIO S3.

        Args:
            file_path: Local path to the file
            bucket_name: Target bucket name
            s3_key: S3 object key (defaults to filename)

        Returns:
            True if upload was successful
        """
        if s3_key is None:
            s3_key = file_path.name

        try:
            file_size = file_path.stat().st_size
            print(f"  Uploading {file_path.name} ({file_size:,} bytes)...", end=" ")

            self.s3_client.upload_file(
                str(file_path),
                bucket_name,
                s3_key,
            )

            print(f"✓ Uploaded to s3://{bucket_name}/{s3_key}")
            return True

        except ClientError as e:
            print(f"✗ Failed: {e}")
            return False
        except Exception as e:
            print(f"✗ Error: {e}")
            return False

    def list_csv_files(self, directory: Path) -> List[Path]:
        """
        Find all CSV files in the given directory.

        Args:
            directory: Directory to search

        Returns:
            List of CSV file paths
        """
        if not directory.exists():
            print(f"✗ Directory not found: {directory}")
            return []

        csv_files = list(directory.glob("*.csv"))
        return sorted(csv_files)

    def upload_all_csv(
        self, source_dir: Path, bucket_name: str, prefix: str = ""
    ) -> dict:
        """
        Upload all CSV files from a directory to MinIO.

        Args:
            source_dir: Source directory containing CSV files
            bucket_name: Target bucket name
            prefix: Optional S3 key prefix (folder path)

        Returns:
            Dictionary with upload statistics
        """
        csv_files = self.list_csv_files(source_dir)

        if not csv_files:
            print(f"✗ No CSV files found in {source_dir}")
            return {"total": 0, "success": 0, "failed": 0}

        print(f"\nFound {len(csv_files)} CSV file(s) in {source_dir}")
        print("-" * 80)

        # Ensure bucket exists
        if not self.ensure_bucket_exists(bucket_name):
            return {"total": len(csv_files), "success": 0, "failed": len(csv_files)}

        print(f"\nUploading to s3://{bucket_name}/{prefix if prefix else ''}")
        print("-" * 80)

        # Upload files
        success_count = 0
        failed_count = 0

        for csv_file in csv_files:
            s3_key = f"{prefix}/{csv_file.name}" if prefix else csv_file.name
            if self.upload_file(csv_file, bucket_name, s3_key):
                success_count += 1
            else:
                failed_count += 1

        return {
            "total": len(csv_files),
            "success": success_count,
            "failed": failed_count,
        }


def main():
    """Main entry point for the upload script."""
    # Get configuration from environment variables or use defaults
    endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minio_root")
    secret_key = os.getenv("MINIO_SECRET_KEY", "m1n1opwd")
    bucket_name = os.getenv("MINIO_BUCKET", "data")
    use_ssl = os.getenv("MINIO_USE_SSL", "false").lower() == "true"

    # Determine project root and raw_data directory
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    raw_data_dir = project_root / "raw_data"

    print("=" * 80)
    print("MinIO CSV Uploader")
    print("=" * 80)
    print(f"Endpoint:      {endpoint}")
    print(f"Bucket:        {bucket_name}")
    print(f"Source dir:    {raw_data_dir}")
    print(f"Use SSL:       {use_ssl}")
    print("=" * 80)

    # Initialize uploader
    try:
        uploader = MinIOUploader(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            use_ssl=use_ssl,
        )
    except Exception as e:
        print(f"✗ Failed to initialize MinIO client: {e}")
        sys.exit(1)

    # Upload all CSV files
    results = uploader.upload_all_csv(raw_data_dir, bucket_name)

    # Print summary
    print("\n" + "=" * 80)
    print("Upload Summary")
    print("=" * 80)
    print(f"Total files:    {results['total']}")
    print(f"Successful:     {results['success']}")
    print(f"Failed:         {results['failed']}")
    print("=" * 80)

    # Exit with appropriate code
    if results["failed"] > 0:
        print("\n⚠ Some uploads failed!")
        sys.exit(1)
    elif results["success"] > 0:
        print("\n✓ All uploads completed successfully!")
        sys.exit(0)
    else:
        print("\n⚠ No files were uploaded")
        sys.exit(0)


if __name__ == "__main__":
    main()
