"""
src/loaders/postgres.py

Handles loading parquet data into local PostgreSQL.
Used by the local development DAG (medicare_enrollment_load_postgres.py).
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

# Airflow connection ID for local postgres
CONN_ID = "medicare_postgres"


def get_latest_parquet(data_dir: Path) -> Path:
    """
    Find the most recent parquet file under data_dir by folder name (YYYY-MM).
    Automatically picks up new months as they are added.
    """
    parquet_files = sorted(data_dir.glob("*/*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found under {data_dir}")
    latest = parquet_files[-1]
    logger.info("Latest parquet file: %s", latest)
    return latest


def load_parquet_to_postgres(
    parquet_path: Path,
    table: str,
    schema: str = "public",
    conn_id: str = CONN_ID,
    chunksize: int = 1000,
) -> int:
    """
    Load a parquet file into PostgreSQL using a full reload strategy.
    Drops and recreates the table each run.

    Args:
        parquet_path: Path to the parquet file.
        table: Target table name.
        schema: Target schema name.
        conn_id: Airflow connection ID for postgres.
        chunksize: Number of rows per insert batch.

    Returns:
        Row count loaded.
    """
    logger.info("Reading parquet file: %s", parquet_path)
    df = pd.read_parquet(parquet_path, engine="pyarrow")
    logger.info("Rows read: %d  |  Columns: %d", len(df), len(df.columns))

    # Lowercase column names for postgres convention
    df.columns = [c.lower() for c in df.columns]

    hook = PostgresHook(postgres_conn_id=conn_id)
    engine = hook.get_sqlalchemy_engine()

    df.to_sql(
        name=table,
        con=engine,
        schema=schema,
        if_exists="replace",
        index=False,
        chunksize=chunksize,
        method="multi",
    )

    # Verify row count
    row_count = hook.get_first(
        f"SELECT COUNT(*) FROM {schema}.{table}"
    )[0]
    logger.info(
        "Loaded %d rows into %s.%s", row_count, schema, table
    )
    return row_count
