"""
dags/medicare_enrollment_load.py

Loads Medicare Monthly Enrollment data from a local parquet file into PostgreSQL.

Trigger: Manual only (no schedule)
Strategy: Full reload — drops and recreates the table each run

Usage:
    Trigger manually from the Airflow UI or CLI:
    docker exec -it airflow_webserver airflow dags trigger medicare_enrollment_load
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

# -----------------------------
# Config
# -----------------------------
CONN_ID = "medicare_postgres"
TARGET_TABLE = "medicare_monthly_enrollment"
TARGET_SCHEMA = "public"
DATA_DIR = Path("/opt/airflow/data/raw/enrollment")


def get_latest_parquet() -> Path:
    """
    Find the most recent parquet file under DATA_DIR by folder name (YYYY-MM).
    This means when 2025-12, 2026-01, etc. are added, the DAG automatically
    picks up the latest month without any changes.
    """
    parquet_files = sorted(DATA_DIR.glob("*/*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found under {DATA_DIR}")
    latest = parquet_files[-1]
    return latest


def load_to_postgres() -> None:
    """
    Read the latest parquet file and do a full reload into postgres.
    Drops and recreates the table each run.
    """
    # Find file
    parquet_path = get_latest_parquet()
    print(f"Loading file: {parquet_path}")

    # Read parquet
    df = pd.read_parquet(parquet_path, engine="pyarrow")
    print(f"Rows read: {len(df):,}  |  Columns: {len(df.columns)}")

    # Lowercase column names for postgres convention
    df.columns = [c.lower() for c in df.columns]

    # Write to postgres
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    df.to_sql(
        name=TARGET_TABLE,
        con=engine,
        schema=TARGET_SCHEMA,
        if_exists="replace",   # full reload — drops and recreates each run
        index=False,
        chunksize=1000,
        method="multi",        # batches inserts for performance
    )

    print(f"Successfully loaded {len(df):,} rows into {TARGET_SCHEMA}.{TARGET_TABLE}")

    # Quick sanity check
    row_count = hook.get_first(f"SELECT COUNT(*) FROM {TARGET_SCHEMA}.{TARGET_TABLE}")[0]
    print(f"Row count confirmed in postgres: {row_count:,}")


# -----------------------------
# DAG definition
# -----------------------------
with DAG(
    dag_id="medicare_enrollment_load",
    description="Full reload of Medicare Monthly Enrollment data into PostgreSQL",
    schedule_interval=None,   # manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["medicare", "enrollment", "ingestion"],
) as dag:

    load_task = PythonOperator(
        task_id="load_enrollment_to_postgres",
        python_callable=load_to_postgres,
    )