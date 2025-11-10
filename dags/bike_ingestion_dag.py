import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import logging
import json
import numpy as np

# Import the Airflow operators
from airflow.decorators import dag, task
# This is the import for our new operator
from airflow.providers.snowflake.transfers.postgres_to_snowflake import PostgresToSnowflakeOperator

# --- CRITICAL: DOCKER NETWORKING ---
DB_URL = "postgresql://capstone_user:capstone_password@staging-db:5432/staging_data"
# -----------------------------------

# API Endpoints
INFO_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_information.json"
STATUS_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_status.json"

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- Helper Function (from our script) ---
def convert_complex_cols_to_json(df: pd.DataFrame) -> pd.DataFrame:
    def convert_value_to_json(x):
        if isinstance(x, (dict, list)):
            return json.dumps(x)
        if isinstance(x, np.ndarray):
            list_data = [None if pd.isna(item) else item for item in x.tolist()]
            return json.dumps(list_data)
        return x

    for col in list(df.columns):
        if df[col].dtype == 'object':
            is_complex = df[col].dropna().apply(lambda x: isinstance(x, (dict, list, np.ndarray))).any()
            if is_complex:
                logging.info(f"Converting complex column '{col}' to JSON string.")
                df[col] = df[col].apply(convert_value_to_json)
    return df

# --- Airflow Task Definitions ---
@task
def extract_load_station_info():
    """
    Fetches static station info, processes, and replaces the target table.
    """
    logging.info(f"Fetching station information from {INFO_URL}...")
    engine = create_engine(DB_URL)
    
    try:
        response = requests.get(INFO_URL)
        response.raise_for_status()
        data = response.json()
        df = pd.json_normalize(data["data"]["stations"])
        logging.info(f"Successfully fetched and normalized {len(df)} stations.")

        df = convert_complex_cols_to_json(df)

        df.to_sql(
            "raw_station_info",
            con=engine,
            if_exists="replace",
            index=False,
            method="multi",
        )
        logging.info("Successfully loaded station information into 'raw_station_info'.")
    except Exception as e:
        logging.error(f"Error in extract_load_station_info: {e}")
        raise # Re-raise the exception to fail the task in Airflow

@task
def extract_load_station_status():
    """
    Fetches dynamic station status, processes, and appends to the target table.
    """
    logging.info(f"Fetching station status from {STATUS_URL}...")
    engine = create_engine(DB_URL)
    
    try:
        response = requests.get(STATUS_URL)
        response.raise_for_status()
        data = response.json()
        df = pd.json_normalize(data["data"]["stations"])
        df["fetched_at"] = datetime.now()
        logging.info(f"Successfully fetched {len(df)} station statuses.")

        df = convert_complex_cols_to_json(df)

        df.to_sql(
            "raw_station_status",
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
        )
        logging.info("Successfully appended station status to 'raw_station_status'.")
    except Exception as e:
        logging.error(f"Error in extract_load_station_status: {e}")
        raise

# --- DAG Definition ---
@dag(
    dag_id="bike_ingestion_pipeline",
    start_date=datetime(2025, 11, 5),
    schedule="@hourly",
    catchup=False,
    tags=["capstone", "bike_share"],
)
def bike_ingestion_dag():
    """
    DAG to fetch bike-share data, load to Postgres, and then copy to Snowflake.
    """
    
    # --- Task 1: Ingest Info to Postgres ---
    task_load_info_to_pg = extract_load_station_info()

    # --- Task 2: Ingest Status to Postgres ---
    task_load_status_to_pg = extract_load_station_status()

    # --- Task 3: Copy Info from Postgres to Snowflake ---
    task_load_info_pg_to_snow = PostgresToSnowflakeOperator(
        task_id="load_info_pg_to_snow",
        postgres_conn_id="postgres_staging_db", # The new connection we will create
        snowflake_conn_id="snowflake_default",  # The connection you already made
        sql="SELECT * FROM raw_station_info",
        snowflake_table="RAW_STATION_INFO",
        snowflake_stage="BIKE_STAGE",           # The stage we will create in Snowflake
        snowflake_schema="BIKE_SHARE_RAW_DATA"  # Your schema in Snowflake
    )

    # --- Task 4: Copy Status from Postgres to Snowflake ---
    task_load_status_pg_to_snow = PostgresToSnowflakeOperator(
        task_id="load_status_pg_to_snow",
        postgres_conn_id="postgres_staging_db",
        snowflake_conn_id="snowflake_default",
        sql="SELECT * FROM raw_station_status",
        snowflake_table="RAW_STATION_STATUS",
        snowflake_stage="BIKE_STAGE",
        snowflake_schema="BIKE_SHARE_RAW_DATA"
    )

    # --- Set Dependencies ---
    # Run the Snowflake copies *after* the Postgres loads are successful.
    task_load_info_to_pg >> task_load_info_pg_to_snow
    task_load_status_to_pg >> task_load_status_pg_to_snow

# This final line "activates" the DAG
bike_ingestion_dag()