import requests
import pandas as pd
from datetime import datetime, timedelta
import logging
import os
import numpy as np

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator

import clickhouse_connect

# Import shared utility function
from utils.data_utils import convert_complex_cols_to_json

# Connection ID configured in Airflow Admin > Connections
STAGING_DB_CONN_ID = "postgres_staging_db"

# ClickHouse connection settings (from environment variables - see .env.example)
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.environ["CLICKHOUSE_USER"]  # Required - no default
CLICKHOUSE_PASSWORD = os.environ["CLICKHOUSE_PASSWORD"]  # Required - no default
CLICKHOUSE_DATABASE = os.environ["CLICKHOUSE_DATABASE"]  # Required - no default

# Request timeout in seconds (connect_timeout, read_timeout)
REQUEST_TIMEOUT = (10, 30)

INFO_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_information.json"
STATUS_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_status.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def get_clickhouse_client():
    """Get a ClickHouse client connection."""
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )


def pandas_dtype_to_clickhouse(dtype, col_name: str) -> str:
    """Convert pandas dtype to ClickHouse type."""
    dtype_str = str(dtype)
    
    if 'datetime' in dtype_str:
        return 'DateTime'
    elif 'int64' in dtype_str:
        return 'Nullable(Int64)'
    elif 'int32' in dtype_str:
        return 'Nullable(Int32)'
    elif 'float' in dtype_str:
        return 'Nullable(Float64)'
    elif 'bool' in dtype_str:
        return 'Nullable(UInt8)'
    else:
        # Default to String for object types and unknown
        return 'Nullable(String)'


def create_table_from_df(ch_client, table_name: str, df: pd.DataFrame, order_by: list):
    """Dynamically create a ClickHouse table based on DataFrame schema."""
    columns = []
    for col in df.columns:
        ch_type = pandas_dtype_to_clickhouse(df[col].dtype, col)
        # Make order_by columns non-nullable
        if col in order_by:
            ch_type = ch_type.replace('Nullable(', '').replace(')', '')
            if ch_type == 'Nullable':
                ch_type = 'String'
        columns.append(f"`{col}` {ch_type}")
    
    columns_sql = ",\n            ".join(columns)
    order_by_sql = ", ".join([f"`{col}`" for col in order_by])
    
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
            {columns_sql}
    ) ENGINE = MergeTree()
    ORDER BY ({order_by_sql})
    """
    
    logging.info(f"Creating table with SQL:\n{create_sql}")
    ch_client.command(create_sql)


@task(retries=3, retry_delay=timedelta(minutes=2))
def extract_load_station_info():
    logging.info(f"Fetching station information from {INFO_URL}...")
    pg_hook = PostgresHook(postgres_conn_id=STAGING_DB_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()
    r = requests.get(INFO_URL, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    df = pd.json_normalize(data["data"]["stations"])
    logging.info(f"Fetched {len(df)} stations for info feed.")
    df = convert_complex_cols_to_json(df)
    df.to_sql("raw_station_info", con=engine, if_exists="replace", index=False, method="multi")
    logging.info("Loaded station information into raw_station_info.")


@task(retries=3, retry_delay=timedelta(minutes=2))
def extract_load_station_status():
    logging.info(f"Fetching station status from {STATUS_URL}...")
    pg_hook = PostgresHook(postgres_conn_id=STAGING_DB_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()
    r = requests.get(STATUS_URL, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    df = pd.json_normalize(data["data"]["stations"])
    df["fetched_at"] = datetime.now()
    logging.info(f"Fetched {len(df)} station status rows.")
    df = convert_complex_cols_to_json(df)
    df.to_sql("raw_station_status", con=engine, if_exists="append", index=False, method="multi")
    logging.info("Appended station status into raw_station_status.")


@task(retries=2, retry_delay=timedelta(minutes=1))
def load_pg_table_to_clickhouse(pg_table: str, clickhouse_table: str, if_exists: str = "replace"):
    """Load data from PostgreSQL staging to ClickHouse analytics database."""
    pg = PostgresHook(postgres_conn_id="postgres_staging_db")
    ch_client = get_clickhouse_client()
    pg_engine = pg.get_sqlalchemy_engine()

    target_lower = clickhouse_table.lower()

    if target_lower == "raw_station_info":
        logging.info(f"Full refresh of {clickhouse_table} from {pg_table}")
        df = pg.get_pandas_df(f"SELECT * FROM {pg_table}")
        if df.empty:
            logging.warning(f"No data in {pg_table}; skipping load to {clickhouse_table}")
            return

        # Convert any problematic types
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).replace('None', '')
        
        # Drop and recreate table dynamically
        ch_client.command(f"DROP TABLE IF EXISTS {clickhouse_table}")
        create_table_from_df(ch_client, clickhouse_table, df, order_by=['station_id'])
        
        # Insert data
        ch_client.insert_df(clickhouse_table, df)
        logging.info(f"Loaded {len(df)} rows into {clickhouse_table}")
        return

    if target_lower == "raw_station_status":
        logging.info(f"Incremental chunked load of {clickhouse_table} from {pg_table}")

        # Get a sample to determine schema
        sample_df = pg.get_pandas_df(f"SELECT * FROM {pg_table} LIMIT 1")
        if sample_df.empty:
            logging.warning(f"No data in {pg_table}; skipping")
            return
            
        # Convert types
        for col in sample_df.columns:
            if sample_df[col].dtype == 'object':
                sample_df[col] = sample_df[col].astype(str)

        # Create table if not exists
        try:
            ch_client.query(f"SELECT 1 FROM {clickhouse_table} LIMIT 1")
            table_exists = True
        except:
            table_exists = False
            
        if not table_exists:
            create_table_from_df(ch_client, clickhouse_table, sample_df, order_by=['station_id', 'fetched_at'])

        # Get last timestamp from ClickHouse
        last_ts = None
        try:
            result = ch_client.query(f"SELECT max(fetched_at) FROM {clickhouse_table}")
            if result.result_rows and result.result_rows[0][0]:
                last_ts = result.result_rows[0][0]
        except Exception as e:
            logging.warning(f"Could not get max(fetched_at) from {clickhouse_table}, assuming empty. Error: {e}")
            last_ts = None

        if last_ts is None:
            query = f"SELECT * FROM {pg_table}"
        else:
            query = f"SELECT * FROM {pg_table} WHERE fetched_at > '{last_ts}'"

        total_loaded = 0
        chunk_size = 20000

        for chunk_df in pd.read_sql(query, con=pg_engine, chunksize=chunk_size):
            if chunk_df.empty:
                continue
            # Convert object columns to string
            for col in chunk_df.columns:
                if chunk_df[col].dtype == 'object':
                    chunk_df[col] = chunk_df[col].astype(str).replace('None', '')
            ch_client.insert_df(clickhouse_table, chunk_df)
            total_loaded += len(chunk_df)

        if total_loaded == 0:
            logging.info(f"No new rows in {pg_table} since {last_ts}; nothing loaded into {clickhouse_table}")
        else:
            logging.info(f"Loaded {total_loaded} new rows into {clickhouse_table}")
        return

    # Default load behavior
    logging.info(f"Default load for {clickhouse_table} from {pg_table} with if_exists={if_exists}")
    df = pg.get_pandas_df(f"SELECT * FROM {pg_table}")
    if df.empty:
        logging.warning(f"No data in {pg_table}; skipping load to {clickhouse_table}")
        return

    if if_exists == "replace":
        ch_client.command(f"DROP TABLE IF EXISTS {clickhouse_table}")
    
    ch_client.insert_df(clickhouse_table, df)
    logging.info(f"Loaded {len(df)} rows into {clickhouse_table}")


@dag(
    dag_id="bike_ingestion_pipeline",
    start_date=datetime(2025, 11, 5),
    schedule="@hourly",
    catchup=False,
    tags=["capstone", "bike_share"],
)
def bike_ingestion_dag():
    info_pg = extract_load_station_info()
    status_pg = extract_load_station_status()

    info_to_ch = load_pg_table_to_clickhouse.override(task_id="load_info_pg_to_clickhouse")(
        pg_table="raw_station_info",
        clickhouse_table="raw_station_info",
        if_exists="replace",
    )
    status_to_ch = load_pg_table_to_clickhouse.override(task_id="load_status_pg_to_clickhouse")(
        pg_table="raw_station_status",
        clickhouse_table="raw_station_status",
        if_exists="append",
    )
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt && /opt/airflow/dbt_venv/bin/dbt run --target dev",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && /opt/airflow/dbt_venv/bin/dbt test --target dev",
    )

    info_pg >> info_to_ch
    status_pg >> status_to_ch

    [info_to_ch, status_to_ch] >> dbt_run >> dbt_test

bike_ingestion_dag()
