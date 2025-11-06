import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import logging

# --- Setup ---
# Set up basic logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Database Connection String
# (Make sure your 'staging-db' container is running!)
DB_URL = "postgresql://capstone_user:capstone_password@localhost:5433/staging_data"

# API Endpoints for Citi Bike NYC
INFO_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_information.json"
STATUS_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_status.json"

try:
    engine = create_engine(DB_URL)
    logging.info("Successfully connected to the staging database.")
except Exception as e:
    logging.error(f"Error connecting to database: {e}")
    exit(1)


def fetch_and_load_station_info():
    """
    Fetches the static station information and loads it into the database.
    This table is loaded with 'if_exists='replace'' because it's a
    full refresh of dimension data.
    """
    try:
        logging.info(f"Fetching station information from {INFO_URL}...")
        response = requests.get(INFO_URL)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        data = response.json()

        # Normalize the nested JSON data
        df = pd.json_normalize(data["data"]["stations"])
        logging.info(f"Successfully fetched and normalized {len(df)} stations.")

        # Load the DataFrame into PostgreSQL
        # We replace this table every time since it's a full snapshot
        df.to_sql(
            "raw_station_info",
            con=engine,
            if_exists="replace",
            index=False,
            method="multi",  # Efficiently inserts multiple rows
        )
        logging.info(
            "Successfully loaded station information into 'raw_station_info' table."
        )

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from API: {e}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")


def fetch_and_load_station_status():
    """
    Fetches the dynamic station status and appends it to the database.
    This table is loaded with 'if_exists='append'' to build history.
    """
    try:
        logging.info(f"Fetching station status from {STATUS_URL}...")
        response = requests.get(STATUS_URL)
        response.raise_for_status()
        data = response.json()

        # Normalize the nested JSON data
        df = pd.json_normalize(data["data"]["stations"])

        # --- THIS IS THE CRITICAL STEP ---
        # Add a new column for the exact time of this fetch
        df["fetched_at"] = datetime.now()
        # ---------------------------------

        logging.info(f"Successfully fetched {len(df)} station statuses.")

        # Load the DataFrame into PostgreSQL
        # We append this data to build a historical record
        df.to_sql(
            "raw_station_status",
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
        )
        logging.info(
            "Successfully appended station status to 'raw_station_status' table."
        )

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from API: {e}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")


if __name__ == "__main__":
    logging.info("--- Starting Data Ingestion ---")
    
    # Run both for the first time to populate tables
    fetch_and_load_station_info()
    fetch_and_load_station_status()
    
    logging.info("--- Data Ingestion Complete ---")