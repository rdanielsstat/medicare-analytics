"""
dags/medicare_enrollment_pipeline_postgres.py

End-to-end Medicare Monthly Enrollment pipeline:
    1. download_from_api     — fetch latest data from CMS API, save as parquet
    2. validate_raw_data     — check row counts and expected columns
    3. load_to_postgres      — full reload into postgres
    4. run_dbt_transforms    — refresh dbt staging + mart models
    5. validate_mart         — assert mart has expected row count

Trigger: Manual only (no schedule)
Usage:
    docker exec -it airflow_webserver airflow dags trigger medicare_enrollment_pipeline_postgres
"""
from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

# -----------------------------
# Config
# -----------------------------
CONN_ID = "medicare_postgres"
TARGET_TABLE = "medicare_monthly_enrollment"
TARGET_SCHEMA = "public"
DATA_DIR = Path("/opt/airflow/data/raw/enrollment")
DBT_PROJECT_DIR = "/opt/airflow/dbt/medicare_dbt"

API_URL = "https://data.cms.gov/data-api/v1/dataset/d7fabe1e-d19b-4333-9eff-e80e0643f2fd/data"
NUMERIC_COLS = [
    "TOT_BENES",
    "ORGNL_MDCR_BENES",
    "MA_AND_OTH_BENES",
    "AGED_TOT_BENES",
    "AGED_ESRD_BENES",
    "AGED_NO_ESRD_BENES",
    "DSBLD_TOT_BENES",
    "DSBLD_ESRD_AND_ESRD_ONLY_BENES",
    "DSBLD_NO_ESRD_BENES",
    "MALE_TOT_BENES",
    "FEMALE_TOT_BENES",
    "WHITE_TOT_BENES",
    "BLACK_TOT_BENES",
    "API_TOT_BENES",
    "HSPNC_TOT_BENES",
    "NATIND_TOT_BENES",
    "OTHR_TOT_BENES",
    "AGE_LT_25_BENES",
    "AGE_25_TO_44_BENES",
    "AGE_45_TO_64_BENES",
    "AGE_65_TO_69_BENES",
    "AGE_70_TO_74_BENES",
    "AGE_75_TO_79_BENES",
    "AGE_80_TO_84_BENES",
    "AGE_85_TO_89_BENES",
    "AGE_90_TO_94_BENES",
    "AGE_GT_94_BENES",
    "DUAL_TOT_BENES",
    "FULL_DUAL_TOT_BENES",
    "PART_DUAL_TOT_BENES",
    "NODUAL_TOT_BENES",
    "QMB_ONLY_BENES",
    "QMB_PLUS_BENES",
    "SLMB_ONLY_BENES",
    "SLMB_PLUS_BENES",
    "QDWI_QI_BENES",
    "OTHR_FULL_DUAL_MDCD_BENES",
    "A_B_TOT_BENES",
    "A_B_ORGNL_MDCR_BENES",
    "A_B_MA_AND_OTH_BENES",
    "A_TOT_BENES",
    "A_ORGNL_MDCR_BENES",
    "A_MA_AND_OTH_BENES",
    "B_TOT_BENES",
    "B_ORGNL_MDCR_BENES",
    "B_MA_AND_OTH_BENES",
    "PRSCRPTN_DRUG_TOT_BENES",
    "PRSCRPTN_DRUG_PDP_BENES",
    "PRSCRPTN_DRUG_MAPD_BENES",
    "PRSCRPTN_DRUG_DEEMED_ELIGIBLE_FULL_LIS_BENES",
    "PRSCRPTN_DRUG_FULL_LIS_BENES",
    "PRSCRPTN_DRUG_PARTIAL_LIS_BENES",
    "PRSCRPTN_DRUG_NO_LIS_BENES",
]

# -----------------------------
# Task functions
# -----------------------------
def download_from_api(**context) -> str:
    """Fetch latest data from CMS API and save as parquet. Returns path to file."""
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    # Get release_month from dag_run config
    release_month = context["dag_run"].conf.get("release_month")
    force = context["dag_run"].conf.get("force", False)

    if not release_month:
        raise ValueError("release_month must be passed in dag_run config, e.g. {'release_month': '2025-11'}")

    out_dir = DATA_DIR / release_month
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"medicare_monthly_enrollment_{release_month}.parquet"

    if out_path.exists() and not force:
        logger.info("File already exists at %s — skipping download.", out_path)
        context["ti"].xcom_push(key="parquet_path", value=str(out_path))
        return str(out_path)

    # Build session with retries
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=1.0, status_forcelist=(429, 500, 502, 503, 504))
    session.mount("https://", HTTPAdapter(max_retries=retry))

    # Paginate
    page_size = context["dag_run"].conf.get("page_size", 5000)
    all_rows, offset = [], 0
    while True:
        response = session.get(API_URL, params={"offset": offset, "size": page_size}, timeout=60)
        response.raise_for_status()
        page = response.json()
        if not page:
            break
        all_rows.extend(page)
        offset += len(page)
        logger.info("Fetched %d rows so far", len(all_rows))

    # Build dataframe
    df = pd.DataFrame(all_rows)
    cols_to_cast = [c for c in NUMERIC_COLS if c in df.columns]
    df[cols_to_cast] = df[cols_to_cast].apply(pd.to_numeric, errors="coerce")

    df.to_parquet(out_path, engine="pyarrow", index=False, compression="snappy")
    logger.info("Saved %d rows to %s", len(df), out_path)

    context["ti"].xcom_push(key="parquet_path", value=str(out_path))
    return str(out_path)

def validate_raw_data(**context) -> None:
    """Check the parquet file has expected shape and columns before loading."""
    parquet_path = context["ti"].xcom_pull(task_ids="download_from_api", key="parquet_path")
    df = pd.read_parquet(parquet_path)

    # Row count check
    min_expected_rows = 100_000
    assert len(df) >= min_expected_rows, (
        f"Row count too low: {len(df):,} — expected at least {min_expected_rows:,}. "
        "Possible incomplete download."
    )

    # Column check
    missing = set(c.lower() for c in NUMERIC_COLS) - set(df.columns.str.lower())
    assert not missing, f"Missing expected columns (schema drift?): {missing}"

    logger.info("Validation passed: %d rows, %d columns", len(df), len(df.columns))


def load_to_postgres(**context) -> None:
    """Full reload of parquet into postgres."""
    parquet_path = context["ti"].xcom_pull(task_ids="download_from_api", key="parquet_path")
    df = pd.read_parquet(parquet_path, engine="pyarrow")
    df.columns = [c.lower() for c in df.columns]

    hook = PostgresHook(postgres_conn_id=CONN_ID)
    
    # Drop with cascade to remove dependent dbt views first
    hook.run(f"DROP TABLE IF EXISTS {TARGET_SCHEMA}.{TARGET_TABLE} CASCADE")

    engine = hook.get_sqlalchemy_engine()
    df.to_sql(
        name=TARGET_TABLE,
        con=engine,
        schema=TARGET_SCHEMA,
        if_exists="replace",
        index=False,
        chunksize=1000,
        method="multi",
    )

    row_count = hook.get_first(f"SELECT COUNT(*) FROM {TARGET_SCHEMA}.{TARGET_TABLE}")[0]
    logger.info("Loaded %d rows into %s.%s", row_count, TARGET_SCHEMA, TARGET_TABLE)
    assert row_count == len(df), f"Row count mismatch: postgres={row_count}, parquet={len(df)}"


def validate_mart(**context) -> None:
    """Assert the dbt mart has a reasonable number of national monthly rows."""
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    row_count = hook.get_first(
        "SELECT COUNT(*) FROM dbt_medicare.mart_enrollment_national"
    )[0]

    min_expected = 100  # ~12 years * 12 months
    assert row_count >= min_expected, (
        f"Mart row count too low: {row_count} — expected at least {min_expected}"
    )
    logger.info("Mart validation passed: %d rows", row_count)


# -----------------------------
# DAG definition
# -----------------------------
with DAG(
    dag_id="medicare_enrollment_pipeline_postgres",
    description="End-to-end Medicare enrollment pipeline",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["medicare", "enrollment", "pipeline"],
) as dag:

    t1 = PythonOperator(
        task_id="download_from_api",
        python_callable=download_from_api,
    )

    t2 = PythonOperator(
        task_id="validate_raw_data",
        python_callable=validate_raw_data,
    )

    t3 = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
    )

    t4 = BashOperator(
        task_id="run_dbt_transforms",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir /opt/airflow/dbt/profiles",
    )

    t5 = PythonOperator(
        task_id="validate_mart",
        python_callable=validate_mart,
    )

    t1 >> t2 >> t3 >> t4 >> t5