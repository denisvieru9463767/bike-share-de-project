import requests
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import logging
import json
import numpy as np

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.bash import BashOperator

DB_URL = "postgresql://capstone_user:capstone_password@staging-db:5432/staging_data"

INFO_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_information.json"
STATUS_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_status.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def convert_complex_cols_to_json(df: pd.DataFrame) -> pd.DataFrame:
    def convert_value_to_json(x):
        if isinstance(x, (dict, list)):
            return json.dumps(x)
        if isinstance(x, np.ndarray):
            lst = [None if pd.isna(i) else i for i in x.tolist()]
            return json.dumps(lst)
        return x
    for col in list(df.columns):
        if df[col].dtype == "object":
            is_complex = df[col].dropna().apply(lambda v: isinstance(v, (dict, list, np.ndarray))).any()
            if is_complex:
                logging.info(f"Converting complex column '{col}' to JSON string.")
                df[col] = df[col].apply(convert_value_to_json)
    return df

@task
def extract_load_station_info():
    logging.info(f"Fetching station information from {INFO_URL}...")
    engine = create_engine(DB_URL)
    r = requests.get(INFO_URL)
    r.raise_for_status()
    data = r.json()
    df = pd.json_normalize(data["data"]["stations"])
    logging.info(f"Fetched {len(df)} stations for info feed.")
    df = convert_complex_cols_to_json(df)
    df.to_sql("raw_station_info", con=engine, if_exists="replace", index=False, method="multi")
    logging.info("Loaded station information into raw_station_info.")

@task
def extract_load_station_status():
    logging.info(f"Fetching station status from {STATUS_URL}...")
    engine = create_engine(DB_URL)
    r = requests.get(STATUS_URL)
    r.raise_for_status()
    data = r.json()
    df = pd.json_normalize(data["data"]["stations"])
    df["fetched_at"] = datetime.now()
    logging.info(f"Fetched {len(df)} station status rows.")
    df = convert_complex_cols_to_json(df)
    df.to_sql("raw_station_status", con=engine, if_exists="append", index=False, method="multi")
    logging.info("Appended station status into raw_station_status.")

@task
def load_pg_table_to_snowflake(pg_table: str, snowflake_table: str, if_exists: str = "replace"):
    pg = PostgresHook(postgres_conn_id="postgres_staging_db")
    sf = SnowflakeHook(snowflake_conn_id="snowflake_default")
    sf_engine = sf.get_sqlalchemy_engine()
    pg_engine = pg.get_sqlalchemy_engine()

    target_upper = snowflake_table.upper()

    if target_upper == "RAW_STATION_INFO":
        logging.info(f"Full refresh of {snowflake_table} from {pg_table}")
        df = pg.get_pandas_df(f"SELECT * FROM {pg_table}")
        if df.empty:
            logging.warning(f"No data in {pg_table}; skipping load to {snowflake_table}")
            return
        with sf_engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {snowflake_table}"))
            df.to_sql(
                name=snowflake_table,
                con=conn,
                if_exists="append",
                index=False,
                method="multi",
            )
        logging.info(f"Loaded {len(df)} rows into {snowflake_table}")
        return

    if target_upper == "RAW_STATION_STATUS":
        logging.info(f"Incremental chunked load of {snowflake_table} from {pg_table}")

        last_ts = None
        try:
            with sf.get_conn() as conn:
                cur = conn.cursor()
                cur.execute(f"SELECT MAX(fetched_at) FROM {snowflake_table}")
                row = cur.fetchone()
                last_ts = row[0] if row and row[0] is not None else None
        except Exception as e:
            logging.warning(f"Could not get max(fetched_at) from {snowflake_table}, assuming empty. Error: {e}")
            last_ts = None

        if last_ts is None:
            query = text(f"SELECT * FROM {pg_table}")
            params = {}
        else:
            query = text(f"SELECT * FROM {pg_table} WHERE fetched_at > :last_ts")
            params = {"last_ts": last_ts}

        total_loaded = 0
        chunk_size = 20000

        with sf_engine.begin() as sf_conn:
            for chunk_df in pd.read_sql(query, con=pg_engine, params=params, chunksize=chunk_size):
                if chunk_df.empty:
                    continue
                chunk_df.to_sql(
                    name=snowflake_table,
                    con=sf_conn,
                    if_exists="append",
                    index=False,
                    method="multi",
                )
                total_loaded += len(chunk_df)

        if total_loaded == 0:
            logging.info(f"No new rows in {pg_table} since {last_ts}; nothing loaded into {snowflake_table}")
        else:
            logging.info(f"Loaded {total_loaded} new rows into {snowflake_table}")
        return

    logging.info(f"Default load for {snowflake_table} from {pg_table} with if_exists={if_exists}")
    df = pg.get_pandas_df(f"SELECT * FROM {pg_table}")
    if df.empty:
        logging.warning(f"No data in {pg_table}; skipping load to {snowflake_table}")
        return

    with sf_engine.begin() as conn:
        if if_exists == "replace":
            conn.execute(text(f"DROP TABLE IF EXISTS {snowflake_table}"))
            target_if_exists = "append"
        else:
            target_if_exists = if_exists

        df.to_sql(
            name=snowflake_table,
            con=conn,
            if_exists=target_if_exists,
            index=False,
            method="multi",
        )

    logging.info(f"Loaded {len(df)} rows into {snowflake_table}")


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

    info_to_snow = load_pg_table_to_snowflake.override(task_id="load_info_pg_to_snow")(
        pg_table="raw_station_info",
        snowflake_table="RAW_STATION_INFO",
        if_exists="replace",
    )
    status_to_snow = load_pg_table_to_snowflake.override(task_id="load_status_pg_to_snow")(
        pg_table="raw_station_status",
        snowflake_table="RAW_STATION_STATUS",
        if_exists="append",
    )
    dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command="cd /opt/airflow/dbt && /opt/airflow/dbt_venv/bin/dbt run --target dev",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && /opt/airflow/dbt_venv/bin/dbt run --target dev",
    )

    info_pg >> info_to_snow
    status_pg >> status_to_snow

    [info_to_snow, status_to_snow] >> dbt_run >> dbt_test

bike_ingestion_dag()
