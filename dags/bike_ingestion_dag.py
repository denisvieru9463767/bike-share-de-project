import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import logging
import json
import numpy as np

# Import the Airflow operators
from airflow.decorators import dag, task

# --- CRITICAL: DOCKER NETWORKING ---
# This is the *most important* change.
# When running from your PC, you used 'localhost:5433'.
# When Airflow runs this (from inside a Docker container),
# it must use the *service name* from docker-compose.yml and its *internal* port.
#
# Service name: 'staging-db'
# Internal port: '5432'
#
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
# This function is exactly the same as in your ingest.py
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
# The @task decorator turns a Python function into an Airflow task

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
# The @dag decorator defines the pipeline itself

@dag(
    dag_id="bike_ingestion_pipeline",
    start_date=datetime(2025, 11, 5),  # Use a date in the past
    schedule="@hourly",       # Run once per hour
    catchup=False,                     # Don't run for past intervals
    tags=["capstone", "bike_share"],
)
def bike_ingestion_dag():
    """
    DAG to fetch bike-share data for info and status, and load to Postgres.
    """
    
    # This is how you call the tasks and set their order.
    # Since these two tasks can run at the same time,
    # we just call them. Airflow will run them in parallel.
    
    extract_load_station_info()
    extract_load_station_status()

# This final line "activates" the DAG
bike_ingestion_dag()