#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click


dtype = {
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

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]


@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-password', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-database', default='ny_taxi', help='PostgreSQL database')
@click.option('--year', default=2021, type=int, help='Year of data')
@click.option('--month', default=1, type=int, help='Month of data')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for reading')
@click.option('--data-url', default='', help='Custom parquet file URL')
def run(pg_user, pg_password, pg_host, pg_port, pg_database, year, month, target_table, chunksize, data_url):
    
    if data_url:
        url = data_url
    else:
        url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month:02d}.parquet'

    engine = create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_database}')

    df = pd.read_parquet(url)
    df_iter = [df[i:i+chunksize] for i in range(0, len(df), chunksize)]

    first = True

    for df_chunk in tqdm(df_iter):
        if first:
            df_chunk.head(n=0).to_sql(
                name=target_table, 
                con=engine, 
                if_exists='replace'
            )
            first = False

        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists='append'
        )

    # Load taxi zone lookup table
    zones_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv'
    df_zones = pd.read_csv(zones_url)
    df_zones.to_sql(
        name='taxi_zone_lookup',
        con=engine,
        if_exists='replace',
        index=False
    )
    print("Taxi zone lookup table loaded successfully.")

if __name__ == '__main__':
    run()
