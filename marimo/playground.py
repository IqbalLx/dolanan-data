import marimo

__generated_with = "0.19.4"
app = marimo.App(width="medium")


@app.cell
def _():
    from pyiceberg.catalog import load_catalog

    storage_cgf = {
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "minio_root",
        "s3.secret-access-key": "m1n1opwd",
        "s3.region": "us-west-2",
    }
    catalog_cfg = {
            "type": "sql",
            "uri": "postgresql+psycopg2://postgres:postgres@localhost:5432",
            "warehouse": "s3://warehouse/pg_lake",
            **storage_cgf
        }
    catalog = load_catalog(
        "postgres",
        **catalog_cfg
    )
    return catalog, storage_cgf


@app.cell
def _():
    import os
    import sqlalchemy

    _password = os.environ.get("POSTGRES_PASSWORD", "postgres")
    DATABASE_URL = f"postgresql://postgres:{_password}@localhost:5432/postgres"
    engine = sqlalchemy.create_engine(DATABASE_URL)
    return (sqlalchemy,)


@app.cell
def _(catalog):
    catalog.list_namespaces()
    return


@app.cell
def _(catalog):
    catalog.list_tables(namespace="sqlmesh__intermediate")
    return


@app.cell
def _(catalog):
    table = catalog.load_table("public.yellow_trip_2015")
    return (table,)


@app.cell
def _():
    import polars as pl
    return (pl,)


@app.cell
def _(pl, storage_cgf, table):
    df = pl.scan_iceberg(table.metadata_location, storage_options=storage_cgf)
    return (df,)


@app.cell
def _(df):
    df.head()
    return


@app.cell
def _(df):
    df.describe()
    return


@app.cell
def _(catalog, pl, storage_cgf):
    table2 = catalog.load_table("public.yellow_trip_2016")
    df2 = pl.scan_iceberg(table2.metadata_location, storage_options=storage_cgf)

    df2.head()
    return (df2,)


@app.cell
def _(df):
    df.collect_schema().to_python()
    return


@app.cell
def _(df2):
    df2.collect_schema().to_python()
    return


@app.cell
def _():
    return


@app.cell
def _():
    import sqlalchemy
    return (sqlalchemy,)


@app.cell
def _(sqlalchemy):
    pg = sqlalchemy.create_engine("postgresql+psycopg2://postgres:postgres@localhost:5432/postgres")
    return (pg,)


@app.cell
def _(sqlalchemy):
    non_iceberg_pg = sqlalchemy.create_engine("postgresql+psycopg2://postgres:postgres@localhost:5432/non_iceberg_postgres")
    return (non_iceberg_pg,)


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo, pg):
    _df = mo.sql(
        f"""
        SELECT AVG(passenger_count) FROM intermediate__dev_iqbal.base_yellow_trip 
        """,
        engine=pg
    )
    return


@app.cell
def _(mo, non_iceberg_pg):
    _df = mo.sql(
        f"""
        SELECT AVG(passenger_count) FROM intermediate__dev_iqbal.base_yellow_trip 
        """,
        engine=non_iceberg_pg
    )
    return


@app.cell
def _(mo, pg):
    _df = mo.sql(
        f"""
        SELECT COUNT(*) FROM intermediate__dev_iqbal.base_yellow_trip 
        """,
        engine=pg
    )
    return


@app.cell
def _(mo, pg):
    _df = mo.sql(
        f"""
        SELECT * FROM
        """,
        engine=pg
    )
    return


if __name__ == "__main__":
    app.run()
