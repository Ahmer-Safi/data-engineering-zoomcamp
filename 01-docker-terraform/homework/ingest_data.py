#!/usr/bin/env python
# coding: utf-8

import os
import pandas as pd
import click
from sqlalchemy import create_engine
from tqdm.auto import tqdm

# ------------------------
# SCHEMA (CSV only, tripdata)
# ------------------------
CSV_DTYPE = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

CSV_PARSE_DATES = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

# ------------------------
# INGEST FUNCTIONS
# ------------------------
def ingest_csv(url, engine, target_table, chunksize):
    df_iter = pd.read_csv(
        url,
        dtype=CSV_DTYPE,
        parse_dates=CSV_PARSE_DATES,
        iterator=True,
        chunksize=chunksize
    )

    first_chunk = next(df_iter)
    first_chunk.head(0).to_sql(target_table, engine, if_exists="replace", index=False)
    print(f"Table {target_table} created")

    first_chunk.to_sql(target_table, engine, if_exists="append", index=False)
    print(f"Inserted first chunk: {len(first_chunk)}")

    for chunk in tqdm(df_iter):
        chunk.to_sql(target_table, engine, if_exists="append", index=False)

    print(f"✅ Finished CSV ingestion: {target_table}")


def ingest_parquet(url, engine, target_table):
    df = pd.read_parquet(url)
    df.columns = [c.lower() for c in df.columns]

    df.to_sql(
        target_table,
        engine,
        if_exists="replace",
        index=False,
        chunksize=50_000
    )

    print(f"✅ Finished Parquet ingestion: {target_table}")


def ingest_zone_lookup(url, engine, target_table):
    df = pd.read_csv(url)
    df.columns = [c.lower() for c in df.columns]  # optional: lowercase
    df.to_sql(target_table, engine, if_exists="replace", index=False)
    print(f"✅ Finished zone lookup ingestion: {target_table}")

# ------------------------
# CLI
# ------------------------
@click.command()
@click.option('--pg-user', default=lambda: os.getenv("POSTGRES_USER", "root"))
@click.option('--pg-pass', default=lambda: os.getenv("POSTGRES_PASSWORD", "root"))
@click.option('--pg-host', default=lambda: os.getenv("POSTGRES_HOST", "localhost"))
@click.option('--pg-port', default=lambda: os.getenv("POSTGRES_PORT", "5432"))
@click.option('--pg-db',   default=lambda: os.getenv("POSTGRES_DB", "ny_taxi"))

@click.option('--dataset', required=True, type=click.Choice(["yellow", "green", "zone_lookup"]))
@click.option('--format', 'file_format', required=False, type=click.Choice(["csv", "parquet"]))
@click.option('--year', type=int, required=False)
@click.option('--month', type=int, required=False)
@click.option('--chunksize', default=100_000)
@click.option('--target-table', required=True)
def main(pg_user, pg_pass, pg_host, pg_port, pg_db,
         dataset, file_format, year, month, chunksize, target_table):

    engine = create_engine(
        f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    )

    if dataset == "zone_lookup":
        url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"
        ingest_zone_lookup(url, engine, target_table)

    elif file_format == "csv":
        url = (
            f"https://github.com/DataTalksClub/nyc-tlc-data/"
            f"releases/download/{dataset}/"
            f"{dataset}_tripdata_{year:04d}-{month:02d}.csv.gz"
        )
        ingest_csv(url, engine, target_table, chunksize)

    else:  # parquet
        url = (
            f"https://d37ci6vzurychx.cloudfront.net/trip-data/"
            f"{dataset}_tripdata_{year:04d}-{month:02d}.parquet"
        )
        ingest_parquet(url, engine, target_table)


if __name__ == "__main__":
    main()
