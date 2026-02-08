#!/usr/bin/env python3
"""
NYC Taxi Data Ingestion Script
Loads yellow taxi data from GitHub to PostgreSQL database.
"""

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click

# Constants and Configuration
DATATYPES = {
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

DATE_COLUMNS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

# Data source
DATA_URL_PREFIX = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'


def create_database_connection(user, password, host, port, database):
    """Create and return a database connection engine."""
    connection_string = (
        f"postgresql+psycopg://{user}:{password}"
        f"@{host}:{port}/{database}"
    )
    return create_engine(connection_string)


def get_data_file_url(year, month):
    """Generate the URL for the data file."""
    return f"{DATA_URL_PREFIX}yellow_tripdata_{year}-{month:02d}.csv.gz"


def display_data_info(df):
    """Display basic information about the DataFrame."""
    print("DataFrame Head:")
    print(df.head())
    print(f"\nTotal Rows: {len(df)}")
    print("\nData Types:")
    print(df.dtypes)


def create_table_schema(engine, df_sample, table_name):
    """Create the table schema in the database."""
    # Display the SQL schema
    schema_sql = pd.io.sql.get_schema(df_sample, name=table_name, con=engine)
    print("Table Schema:")
    print(schema_sql)
    
    # Create empty table
    df_sample.head(n=0).to_sql(
        name=table_name,
        con=engine,
        if_exists='replace'
    )
    print(f"\nCreated empty table: {table_name}")


def ingest_data_in_chunks(engine, data_url, chunk_size, table_name):
    """Ingest data in chunks to manage memory usage."""
    # Create iterator for chunked reading
    df_iterator = pd.read_csv(
        data_url,
        dtype=DATATYPES,
        parse_dates=DATE_COLUMNS,
        iterator=True,
        chunksize=chunk_size
    )
    
    is_first_chunk = True
    total_rows_processed = 0
    
    print(f"\nStarting data ingestion with chunk size: {chunk_size:,}")
    
    for chunk in tqdm(df_iterator, desc="Processing chunks"):
        if is_first_chunk:
            # Create table from first chunk (schema only)
            chunk.head(0).to_sql(
                name=table_name,
                con=engine,
                if_exists="replace"
            )
            is_first_chunk = False
            print("Table schema created from first chunk")
        
        # Insert chunk into database
        chunk.to_sql(
            name=table_name,
            con=engine,
            if_exists="append",
            index=False  # Don't save DataFrame index as a column
        )
        
        chunk_rows = len(chunk)
        total_rows_processed += chunk_rows
        print(f"Inserted chunk: {chunk_rows:,} rows (Total: {total_rows_processed:,})")
    
    print(f"\nIngestion complete! Total rows inserted: {total_rows_processed:,}")


@click.command()
@click.option('--db-user', default='root', help='Database user')
@click.option('--db-password', default='root', help='Database password')
@click.option('--db-host', default='localhost', help='Database host')
@click.option('--db-port', default=5432, type=int, help='Database port')
@click.option('--db-name', default='ny_taxi', help='Database name')
@click.option('--year', default=2021, type=int, help='Data year')
@click.option('--month', default=1, type=int, help='Data month')
@click.option('--chunk-size', default=100000, type=int, help='Chunk size for data ingestion')
@click.option('--table-name', default='yellow_taxi_data', help='Table name in database')
def main(db_user, db_password, db_host, db_port, db_name, year, month, chunk_size, table_name):
    """Main execution function."""
    print("Starting NYC Taxi Data Ingestion Process")
    print("=" * 50)
    
    # Create database connection
    print("\n1. Creating database connection...")
    engine = create_database_connection(db_user, db_password, db_host, db_port, db_name)
    
    # Generate data URL
    data_url = get_data_file_url(year, month)
    print(f"\n2. Data source: {data_url}")
    
    # Load a sample to examine structure
    print("\n3. Loading data sample for inspection...")
    df_sample = pd.read_csv(
        data_url,
        dtype=DATATYPES,
        parse_dates=DATE_COLUMNS,
        nrows=1000  # Load only sample for inspection
    )
    
    # Display data information
    display_data_info(df_sample)
    
    # Create table schema
    print("\n4. Creating database table schema...")
    create_table_schema(engine, df_sample, table_name)
    
    # Ingest data in chunks
    print("\n5. Starting data ingestion...")
    ingest_data_in_chunks(engine, data_url, chunk_size, table_name)
    
    print("\n" + "=" * 50)
    print("Data ingestion process completed successfully!")


if __name__ == "__main__":
    main()