# Data Platform POC

A proof-of-concept modern data platform demonstrating the integration of Apache Polaris (Iceberg catalog), Trino (query engine), MinIO (object storage), and SQLMesh (data transformation framework).

## Architecture Overview

This project implements a complete data lakehouse architecture with the following components:

### Core Components

- **Apache Polaris**: REST-based Apache Iceberg catalog service that manages metadata for data tables
- **Trino**: Distributed SQL query engine for querying data stored in the lakehouse
- **MinIO**: S3-compatible object storage for storing actual data files
- **SQLMesh**: Data transformation framework for managing SQL-based data models with CI/CD capabilities
- **DuckDB**: Embedded database used for SQLMesh state management

### Architecture Flow

```
┌─────────────┐
│   SQLMesh   │ ─── Transforms data ───┐
└─────────────┘                        │
                                       ▼
┌─────────────┐         ┌──────────────────────┐         ┌─────────────┐
│    Trino    │ ◄─────► │  Apache Polaris      │ ◄─────► │    MinIO    │
│ Query Engine│         │  (Iceberg Catalog)   │         │ (S3 Storage)│
└─────────────┘         └──────────────────────┘         └─────────────┘
      │                                                          │
      └────────────── Reads/Writes Data ────────────────────────┘
```

## Technology Stack

- **Python 3.12+**: Primary programming language
- **SQLMesh**: Data transformation and workflow orchestration
- **DuckDB**: Local state management and development queries
- **Trino**: Production-grade distributed query engine
- **Apache Iceberg**: Table format for large analytical datasets
- **Apache Polaris**: Iceberg catalog implementation
- **MinIO**: S3-compatible object storage
- **Docker Compose**: Container orchestration

## Project Structure

```
data-platform-poc/
├── sqlmesh/                    # SQLMesh project directory
│   ├── models/                 # SQL model definitions
│   │   └── hello.sql          # Example incremental model
│   ├── audits/                # Data quality audits
│   ├── macros/                # Reusable SQL macros
│   ├── seeds/                 # Static seed data
│   ├── tests/                 # Model tests
│   └── config.yaml            # SQLMesh configuration
├── polaris/                   # Polaris setup scripts
│   ├── create-catalog.sh      # Catalog creation script
│   └── obtain-token.sh        # OAuth token retrieval
├── trino/                     # Trino configuration
│   └── catalog/
│       └── nyc_taxi_catalog.properties  # Iceberg catalog config
├── duckdb/                    # DuckDB state databases
├── docker-compose.yml         # Infrastructure definition
├── pyproject.toml            # Python dependencies
└── README.md                 # This file
```

## Features

### SQLMesh Configuration
- **Gateway**: Trino connection for production workloads
- **State Backend**: DuckDB for local state management
- **Model Defaults**: Daily incremental models with Trino dialect
- **Linting**: Enforced SQL quality rules
- **Auto-apply**: Streamlined development workflow with minimal prompts

### Data Catalog
- **Iceberg REST Catalog**: Apache Polaris managing table metadata
- **S3-compatible Storage**: MinIO for distributed object storage
- **OAuth2 Authentication**: Secure catalog access

### Query Engine
- **Distributed Processing**: Trino for scalable SQL queries
- **Iceberg Support**: Native Apache Iceberg table format
- **S3 Integration**: Direct access to MinIO storage

## Prerequisites

- Docker and Docker Compose
- Python 3.12 or higher
- UV package manager (or pip)

## Getting Started

### 1. Start Infrastructure

Launch all services using Docker Compose:

```bash
docker-compose up -d
```

This will start:
- MinIO (ports 9000, 9001)
- Apache Polaris (ports 8181, 5005)
- Trino (port 8080)
- Automatic setup containers for bucket creation and catalog initialization

### 2. Verify Services

Check that all services are healthy:

```bash
docker-compose ps
```

Access the web interfaces:
- **MinIO Console**: http://localhost:9001 (user: `minio_root`, password: `m1n1opwd`)
- **Trino UI**: http://localhost:8080
- **Polaris API**: http://localhost:8181

### 3. Install Python Dependencies

```bash
# Using uv (recommended)
uv sync

# Or using pip
pip install -e .
```

### 4. Run SQLMesh

Activate your virtual environment and run SQLMesh commands:

```bash
# Activate environment
source .venv/bin/activate

# Navigate to SQLMesh project
cd sqlmesh

# Plan and apply transformations
sqlmesh plan

# Run in development environment
sqlmesh plan dev_<your_name>

# Apply to production
sqlmesh plan prod
```

## Configuration Details

### Polaris Catalog

- **Realm**: POLARIS
- **Catalog Name**: nyc_taxi_catalog
- **Storage**: S3 (MinIO) at `s3://warehouse`
- **Credentials**: root:s3cr3t

### Trino Connection

- **Host**: localhost:8080
- **User**: trino
- **Catalog**: nyc_taxi_catalog
- **Authentication**: OAuth2 via Polaris

### MinIO Storage

- **API Endpoint**: http://localhost:9000
- **Console**: http://localhost:9001
- **Access Key**: minio_root
- **Secret Key**: m1n1opwd
- **Buckets**: warehouse, data

## Development Workflow

1. **Create Models**: Add SQL files to `sqlmesh/models/`
2. **Define Transformations**: Use SQLMesh model syntax with incremental strategies
3. **Test Locally**: Run `sqlmesh plan` to preview changes
4. **Deploy**: Apply changes to target environment

### Example Model

```sql
MODEL (
  name staging.hello,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key name
  )
);

SELECT
    'iqbal' as name
```

## Data Quality

SQLMesh linting enforces:
- No ambiguous or invalid columns
- Valid SELECT * expansions
- Unambiguous projections

## Troubleshooting

### Services Not Starting

Check logs for specific services:
```bash
docker-compose logs <service_name>
```

### Catalog Connection Issues

Verify Polaris token:
```bash
docker-compose exec polaris-setup /polaris/obtain-token.sh
```

### Storage Access Problems

Check MinIO bucket permissions in the MinIO console.

## Dependencies

Key Python packages:
- `sqlmesh[dlt,duckdb,trino]` - Data transformation framework with plugins
- `duckdb` - Embedded analytical database
- `ipykernel` - Jupyter notebook support

## Future Enhancements

- [ ] Add dlt (data load tool) pipelines for data ingestion
- [ ] Implement data quality audits
- [ ] Add CI/CD pipeline integration
- [ ] Create example dashboards and analytics
- [ ] Add data lineage visualization
- [ ] Implement role-based access control

## License

This is a proof-of-concept project for evaluation purposes.

## References

- [Apache Polaris Documentation](https://polaris.apache.org/)
- [Trino Documentation](https://trino.io/docs/current/)
- [SQLMesh Documentation](https://sqlmesh.readthedocs.io/)
- [Apache Iceberg](https://iceberg.apache.org/)
- [MinIO Documentation](https://min.io/docs/)