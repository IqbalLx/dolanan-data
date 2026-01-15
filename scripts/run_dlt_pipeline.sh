#!/bin/bash

# Run NYC Taxi dlt Pipeline
# This script runs the dlt-iceberg pipeline to load CSV files from MinIO into Trino

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DLT_DIR="$PROJECT_ROOT/dlthub"

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}NYC Taxi dlt Pipeline Runner${NC}"
echo -e "${GREEN}========================================${NC}"

# Check if dlthub directory exists
if [ ! -d "$DLT_DIR" ]; then
    echo -e "${RED}✗ dlthub directory not found!${NC}"
    exit 1
fi

# Check if pipeline script exists
if [ ! -f "$DLT_DIR/nyc_taxi_pipeline.py" ]; then
    echo -e "${RED}✗ Pipeline script not found!${NC}"
    exit 1
fi

# Check Docker services
echo -e "\n${YELLOW}Checking Docker services...${NC}"

if command -v docker-compose &> /dev/null; then
    # Check MinIO
    if docker-compose ps 2>/dev/null | grep -q "minio.*Up"; then
        echo -e "${GREEN}✓ MinIO service is running${NC}"
    else
        echo -e "${RED}✗ MinIO service is not running${NC}"
        echo -e "${YELLOW}  Starting MinIO...${NC}"
        docker-compose up -d minio
        sleep 3
    fi

    # Check Polaris
    if docker-compose ps 2>/dev/null | grep -q "polaris.*Up"; then
        echo -e "${GREEN}✓ Polaris service is running${NC}"
    else
        echo -e "${RED}✗ Polaris service is not running${NC}"
        echo -e "${YELLOW}  Starting Polaris...${NC}"
        docker-compose up -d polaris
        sleep 5
    fi

    # Check Trino
    if docker-compose ps 2>/dev/null | grep -q "trino.*Up"; then
        echo -e "${GREEN}✓ Trino service is running${NC}"
    else
        echo -e "${YELLOW}⚠ Trino service is not running${NC}"
        echo -e "${YELLOW}  Starting Trino...${NC}"
        docker-compose up -d trino
        sleep 5
    fi
else
    echo -e "${YELLOW}⚠ docker-compose not found, skipping service check${NC}"
fi

# Check for CSV files in MinIO
echo -e "\n${YELLOW}Checking for CSV files in MinIO...${NC}"
if command -v aws &> /dev/null; then
    CSV_COUNT=$(aws --endpoint-url http://localhost:9000 s3 ls s3://data/ 2>/dev/null | grep -c "\.csv$" || echo "0")
    if [ "$CSV_COUNT" -gt 0 ]; then
        echo -e "${GREEN}✓ Found $CSV_COUNT CSV file(s) in s3://data/${NC}"
    else
        echo -e "${RED}✗ No CSV files found in s3://data/${NC}"
        echo -e "${YELLOW}  Run: ./scripts/upload_csv.sh${NC}"
        exit 1
    fi
else
    echo -e "${YELLOW}⚠ AWS CLI not found, skipping CSV check${NC}"
    echo -e "${YELLOW}  Make sure CSV files are uploaded to MinIO${NC}"
fi

# Run the pipeline
echo -e "\n${YELLOW}Running dlt pipeline...${NC}"
cd "$DLT_DIR"

if [ -d "../.venv" ]; then
    # Use virtual environment if it exists
    echo -e "${GREEN}Using virtual environment${NC}"
    source ../.venv/bin/activate
    python nyc_taxi_pipeline.py
elif command -v uv &> /dev/null; then
    # Use uv if available
    echo -e "${GREEN}Using uv${NC}"
    uv run nyc_taxi_pipeline.py
else
    # Fallback to system python
    echo -e "${YELLOW}Using system python${NC}"
    python3 nyc_taxi_pipeline.py
fi

exit_code=$?

if [ $exit_code -eq 0 ]; then
    echo -e "\n${GREEN}========================================${NC}"
    echo -e "${GREEN}✓ Pipeline completed successfully!${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo -e "\n${YELLOW}Next steps:${NC}"
    echo -e "  1. Query data in Trino: http://localhost:8080"
    echo -e "  2. Run SQL: SELECT * FROM nyc_taxi_catalog.raw.yellow_tripdata LIMIT 10;"
    echo -e "  3. Transform with SQLMesh: cd sqlmesh && sqlmesh plan"
else
    echo -e "\n${RED}========================================${NC}"
    echo -e "${RED}✗ Pipeline failed!${NC}"
    echo -e "${RED}========================================${NC}"
    echo -e "\n${YELLOW}Troubleshooting:${NC}"
    echo -e "  1. Check service logs: docker-compose logs polaris minio trino"
    echo -e "  2. Verify connectivity: python -c 'import requests; print(requests.get(\"http://localhost:9000/minio/health/live\").status_code)'"
    echo -e "  3. Check CSV files: aws --endpoint-url http://localhost:9000 s3 ls s3://data/"
fi

exit $exit_code
