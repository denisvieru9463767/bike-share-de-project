import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import logging
import json
import numpy as np  # Ensure numpy is imported

# --- Setup ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
DB_URL = "postgresql://capstone_user:capstone_password@localhost:5433/staging_data"
INFO_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_information.json"
STATUS_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_status.json"

try:
    engine = create_engine(DB_URL)
    logging.info("Successfully connected to the staging database.")
except Exception as e:
    logging.error(f"Error connecting to database: {e}")
    exit(1)


def convert_complex_cols_to_json(df: pd.DataFrame) -> pd.DataFrame:
    """
    Finds object-type columns and converts any dict/list/array
    values to JSON strings, passing others (str, int, nan) through.
    """
    
    def convert_value_to_json(x):
        """
        Helper function for .apply()
        Converts complex types to JSON, passes others through.
        """
        # Check for complex types first
        if isinstance(x, (dict, list)):
            return json.dumps(x)
        
        if isinstance(x, np.ndarray):
            # Convert np.array to list, handling potential NaNs inside
            # by converting them to None, which json.dumps() can handle.
            list_data = [None if pd.isna(item) else item for item in x.tolist()]
            return json.dumps(list_data)
        
        # Return all other types (str, int, float, None, np.nan) as-is.
        # to_sql knows how to handle these (e.g., None/np.nan -> NULL).
        return x

    # Iterate over a copy of columns list to avoid issues while modifying
    for col in list(df.columns):
        if df[col].dtype == 'object':
            # We must apply the conversion to *every* row in any
            # object column, because the column could contain mixed types.
            # We add a check to see if conversion is needed
            # to avoid logging for simple string columns.
            is_complex = df[col].dropna().apply(lambda x: isinstance(x, (dict, list, np.ndarray))).any()
            
            if is_complex:
                logging.info(f"Converting complex column '{col}' to JSON string.")
                df[col] = df[col].apply(convert_value_to_json)
                
    return df


def fetch_and_load_station_info():
    """
    Fetches the static station information and loads it into the database.
    """
    try:
        logging.info(f"Fetching station information from {INFO_URL}...")
        response = requests.get(INFO_URL)
        response.raise_for_status()
        data = response.json()

        df = pd.json_normalize(data["data"]["stations"])
        logging.info(f"Successfully fetched and normalized {len(df)} stations.")

        # --- FIX: Convert complex columns ---
        df = convert_complex_cols_to_json(df)
        # --- END FIX ---

        df.to_sql(
            "raw_station_info",
            con=engine,
            if_exists="replace",
            index=False,
            method="multi",
        )
        logging.info(
            "Successfully loaded station information into 'raw_station_info' table."
        )

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from API: {e}")
    except Exception as e:
        logging.error(f"An error occurred in fetch_and_load_station_info: {e}")


def fetch_and_load_station_status():
    """
    Fetches the dynamic station status and appends it to the database.
    """
    try:
        logging.info(f"Fetching station status from {STATUS_URL}...")
        response = requests.get(STATUS_URL)
        response.raise_for_status()
        data = response.json()

        df = pd.json_normalize(data["data"]["stations"])
        df["fetched_at"] = datetime.now()
        logging.info(f"Successfully fetched {len(df)} station statuses.")

        # --- FIX: Convert complex columns ---
        df = convert_complex_cols_to_json(df)
        # --- END FIX ---

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
        logging.error(f"An error occurred in fetch_and_load_station_status: {e}")


if __name__ == "__main__":
    logging.info("--- Starting Data Ingestion ---")
    fetch_and_load_station_info()
    fetch_and_load_station_status()
    logging.info("--- Data Ingestion Complete ---")