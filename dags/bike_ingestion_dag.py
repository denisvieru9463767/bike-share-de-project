import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import logging
import json
import numpy as np

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# --- CRITICAL: DOCKER NETWORKING ---
DB_URL = "postgresql://capstone_user:capstone_password@staging-db:5432/staging_data"
# -----------------------------------

# API Endpoints
INFO_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_information.json"
STATUS_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_status.json"

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- Helper: convert complex columns to JSON strings ---
def convert_complex_cols_to_json(df: pd.DataFrame) -> pd.DataFrame:
    def convert_value_to_json(x):
        if isinstance(x, (dict, list)):
            return json.dumps(x)
        if isinstance(x, np.ndarray):
            list_data = [None if pd.isna(item) else item for item in x.tolist()]
            return json.dumps(list_data)
        return x

    for col in list(df.columns):
        if df[col].dtype == "object":
            is_complex = df[col].dropna().apply(
                lambda v: isinstance(v, (dict, list, np.ndarray))
            ).any()
            if is_complex:
                logging.info(f"Converting complex column '{col}' to JSON string.")
                df[col] = df[col].apply(convert_value_to_json)
    return df

# --- Task 1: API -> Postgres (station info) ---
@task
def extract_load_station_info():
    logging.info(f"Fetching station information from {INFO_URL}...")
    engine = create_engine(DB_URL)

    response = requests.get(INFO_URL)
    response.raise_for_status()
    data = response.json()

    df = pd.json_normalize(data["data"]["stations"])
    logging.info(f"Fetched {len(df)} stations for info feed.")

    df = convert_complex_cols_to_json(df)

    df.to_sql(
        "raw_station_info",
        con=engine,
        if_exists="replace",
        index=False,
        method="multi",
    )
    logging.info("Loaded station information into raw_station_info.")

# --- Task 2: API -> Postgres (station status) ---
@task
def extract_load_station_status():
    logging.info(f"Fetching station status from {STATUS_URL}...")
    engine = create_engine(DB_URL)

    response = requests.get(STATUS_URL)
    response.raise_for_status()
    data = response.json()

    df = pd.json_normalize(data["data"]["stations"])
    df["fetched_at"] = datetime.now()
    logging.info(f"Fetched {len(df)} station status rows.")

    df = convert_complex_cols_to_json(df)

    df.to_sql(
        "raw_station_status",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
    )
    logging.info("Appended station status into raw_station_status.")

# --- Generic Task: Postgres -> Snowflake using hooks ---
@task
def load_pg_table_to_snowflake(
    pg_table: str,
    snowflake_table: str,
    if_exists: str = "replace",
):
    """
    Simple ELT-style copy:
    - Read full table from Postgres (staging-db)
    - Write to Snowflake table using SnowflakeHook + SQLAlchemy engine.
    Requires:
      - Connection 'postgres_staging_db' in Airflow (Postgres)
      - Connection 'snowflake_default' in Airflow (Snowflake)
      - snowflake-sqlalchemy + snowflake-connector-python installed
    """
    # Read from Postgres
    pg = PostgresHook(postgres_conn_id="postgres_staging_db")
    df = pg.get_pandas_df(f"SELECT * FROM {pg_table}")

    if df.empty:
        logging.warning(f"No data found in {pg_table}; skipping load to Snowflake.")
        return

    # Write to Snowflake
    sf = SnowflakeHook(snowflake_conn_id="snowflake_default")
    engine = sf.get_sqlalchemy_engine()

    df.to_sql(
        name=snowflake_table,
        con=engine,
        if_exists=if_exists,
        index=False,
        method="multi",
    )
    logging.info(
        f"Loaded {len(df)} rows from {pg_table} into Snowflake table {snowflake_table} "
        f"(if_exists={if_exists})."
    )

# --- DAG Definition ---
@dag(
    dag_id="bike_ingestion_pipeline",
    start_date=datetime(2025, 11, 5),
    schedule="@hourly",
    catchup=False,
    tags=["capstone", "bike_share"],
)
def bike_ingestion_dag():
    # API -> Postgres
    info_pg = extract_load_station_info()
    status_pg = extract_load_station_status()

    # Postgres -> Snowflake
    info_to_snow = load_pg_table_to_snowflake.override(
        task_id="load_info_pg_to_snow"
    )(
        pg_table="raw_station_info",
        snowflake_table="RAW_STATION_INFO",
        if_exists="replace",
    )

    status_to_snow = load_pg_table_to_snowflake.override(
        task_id="load_status_pg_to_snow"
    )(
        pg_table="raw_station_status",
        snowflake_table="RAW_STATION_STATUS",
        if_exists="append",
    )

    # Dependencies
    info_pg >> info_to_snow
    status_pg >> status_to_snow

bike_ingestion_dag()
