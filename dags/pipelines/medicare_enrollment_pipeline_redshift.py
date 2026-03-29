"""
dags/medicare_enrollment_pipeline_redshift.py

End-to-end Medicare Monthly Enrollment pipeline for AWS:
    1. download_from_api     — fetch latest data from CMS API, save as parquet
    2. validate_raw_data     — check row counts and expected columns
    3. upload_to_s3          — upload parquet to S3 data lake
    4. load_s3_to_redshift   — COPY from S3 into Redshift Serverless
    5. run_dbt_transforms    — refresh dbt staging + mart models (Redshift target)
    6. validate_mart         — assert mart has expected row count

Trigger: Manual only (no schedule)
Usage:
    docker exec -it airflow_webserver airflow dags trigger medicare_enrollment_pipeline_redshift \
      --conf '{"release_month": "2025-11"}'

Required Airflow Variables:
    S3_BUCKET              — S3 bucket name
    REDSHIFT_WORKGROUP     — Redshift Serverless workgroup name
    REDSHIFT_DATABASE      — Redshift database name
    REDSHIFT_IAM_ROLE      — ARN of IAM role Redshift uses to access S3
    AWS_REGION             — AWS region (default: us-east-1)
    REDSHIFT_ADMIN_USERNAME — Redshift admin username (for dbt)
    REDSHIFT_ADMIN_PASSWORD — Redshift admin password (for dbt)
"""
from __future__ import annotations

import sys
# Make src/ importable inside the Airflow container
sys.path.insert(0, "/opt/airflow")

import logging
import os
from datetime import datetime
from pathlib import Path

import boto3
import pandas as pd
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

# -----------------------------
# Config
# -----------------------------
TARGET_TABLE = "medicare_monthly_enrollment"
TARGET_SCHEMA = "public"
DBT_SCHEMA = "dbt_medicare"
DATA_DIR = Path("/opt/airflow/data/raw/enrollment")
DBT_PROJECT_DIR = "/opt/airflow/dbt/medicare_dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt/profiles"

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


def get_var(key: str, default: str = "") -> str:
    """Read from Airflow Variables with environment variable fallback."""
    try:
        return Variable.get(key)
    except Exception:
        return os.environ.get(key, default)


# -----------------------------
# Task functions
# -----------------------------
def download_from_api(**context) -> str:
    """Fetch latest data from CMS API and save as parquet. Returns path to file."""
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

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

    session = requests.Session()
    retry = Retry(total=5, backoff_factor=1.0, status_forcelist=(429, 500, 502, 503, 504))
    session.mount("https://", HTTPAdapter(max_retries=retry))

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

    min_expected_rows = 100_000
    assert len(df) >= min_expected_rows, (
        f"Row count too low: {len(df):,} — expected at least {min_expected_rows:,}. "
        "Possible incomplete download."
    )

    missing = set(c.lower() for c in NUMERIC_COLS) - set(df.columns.str.lower())
    assert not missing, f"Missing expected columns (schema drift?): {missing}"

    logger.info("Validation passed: %d rows, %d columns", len(df), len(df.columns))


def upload_to_s3(**context) -> str:
    """Upload parquet file to S3 data lake. Skips if already uploaded."""
    parquet_path = Path(context["ti"].xcom_pull(task_ids="download_from_api", key="parquet_path"))
    s3_bucket = get_var("S3_BUCKET")

    # Mirror local folder structure inside S3
    parts = parquet_path.parts
    enrollment_idx = parts.index("enrollment")
    s3_key = "/".join(parts[enrollment_idx:])
    s3_uri = f"s3://{s3_bucket}/{s3_key}"

    s3_client = boto3.client("s3")

    # Skip if already uploaded
    try:
        s3_client.head_object(Bucket=s3_bucket, Key=s3_key)
        logger.info("File already exists in S3, skipping upload: %s", s3_uri)
    except s3_client.exceptions.ClientError:
        logger.info("Uploading %s to %s", parquet_path, s3_uri)
        s3_client.upload_file(str(parquet_path), s3_bucket, s3_key)
        logger.info("Upload complete: %s", s3_uri)

    context["ti"].xcom_push(key="s3_uri", value=s3_uri)
    return s3_uri


def load_s3_to_redshift(**context) -> None:
    """Create table and COPY data from S3 into Redshift Serverless."""
    import time

    s3_uri = context["ti"].xcom_pull(task_ids="upload_to_s3", key="s3_uri")
    workgroup = get_var("REDSHIFT_WORKGROUP", "medicare-analytics-workgroup")
    database = get_var("REDSHIFT_DATABASE", "medicare_db")
    iam_role = get_var("REDSHIFT_IAM_ROLE")
    region = get_var("AWS_REGION", "us-east-1")

    client = boto3.client("redshift-data", region_name=region)

    def run_sql(sql: str) -> None:
        print(f"Running SQL against workgroup={workgroup}, database={database}")
        print(f"SQL: {sql[:200]}")  # first 200 chars
        response = client.execute_statement(
            WorkgroupName=workgroup,
            Database=database,
            Sql=sql,
        )
        statement_id = response["Id"]
        while True:
            desc = client.describe_statement(Id=statement_id)
            status = desc["Status"]
            if status == "FINISHED":
                break
            elif status in ("FAILED", "ABORTED"):
                raise RuntimeError(f"Redshift statement failed: {desc.get('Error')}")
            time.sleep(2)

    # Create table if not exists with distribution and sort keys for query performance
    run_sql(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.{TARGET_TABLE} (
            year                                          VARCHAR(10),
            month                                         VARCHAR(10),
            bene_geo_lvl                                  VARCHAR(50),
            bene_state_abrvtn                             VARCHAR(2),
            bene_state_desc                               VARCHAR(100),
            bene_county_desc                              VARCHAR(100),
            bene_fips_cd                                  VARCHAR(5),
            tot_benes                                     DOUBLE PRECISION,
            orgnl_mdcr_benes                              DOUBLE PRECISION,
            ma_and_oth_benes                              DOUBLE PRECISION,
            aged_tot_benes                                DOUBLE PRECISION,
            aged_esrd_benes                               DOUBLE PRECISION,
            aged_no_esrd_benes                            DOUBLE PRECISION,
            dsbld_tot_benes                               DOUBLE PRECISION,
            dsbld_esrd_and_esrd_only_benes                DOUBLE PRECISION,
            dsbld_no_esrd_benes                           DOUBLE PRECISION,
            male_tot_benes                                DOUBLE PRECISION,
            female_tot_benes                              DOUBLE PRECISION,
            white_tot_benes                               DOUBLE PRECISION,
            black_tot_benes                               DOUBLE PRECISION,
            api_tot_benes                                 DOUBLE PRECISION,
            hspnc_tot_benes                               DOUBLE PRECISION,
            natind_tot_benes                              DOUBLE PRECISION,
            othr_tot_benes                                DOUBLE PRECISION,
            age_lt_25_benes                               DOUBLE PRECISION,
            age_25_to_44_benes                            DOUBLE PRECISION,
            age_45_to_64_benes                            DOUBLE PRECISION,
            age_65_to_69_benes                            DOUBLE PRECISION,
            age_70_to_74_benes                            DOUBLE PRECISION,
            age_75_to_79_benes                            DOUBLE PRECISION,
            age_80_to_84_benes                            DOUBLE PRECISION,
            age_85_to_89_benes                            DOUBLE PRECISION,
            age_90_to_94_benes                            DOUBLE PRECISION,
            age_gt_94_benes                               DOUBLE PRECISION,
            dual_tot_benes                                DOUBLE PRECISION,
            full_dual_tot_benes                           DOUBLE PRECISION,
            part_dual_tot_benes                           DOUBLE PRECISION,
            nodual_tot_benes                              DOUBLE PRECISION,
            qmb_only_benes                                DOUBLE PRECISION,
            qmb_plus_benes                                DOUBLE PRECISION,
            slmb_only_benes                               DOUBLE PRECISION,
            slmb_plus_benes                               DOUBLE PRECISION,
            qdwi_qi_benes                                 DOUBLE PRECISION,
            othr_full_dual_mdcd_benes                     DOUBLE PRECISION,
            a_b_tot_benes                                 DOUBLE PRECISION,
            a_b_orgnl_mdcr_benes                          DOUBLE PRECISION,
            a_b_ma_and_oth_benes                          DOUBLE PRECISION,
            a_tot_benes                                   DOUBLE PRECISION,
            a_orgnl_mdcr_benes                            DOUBLE PRECISION,
            a_ma_and_oth_benes                            DOUBLE PRECISION,
            b_tot_benes                                   DOUBLE PRECISION,
            b_orgnl_mdcr_benes                            DOUBLE PRECISION,
            b_ma_and_oth_benes                            DOUBLE PRECISION,
            prscrptn_drug_tot_benes                       DOUBLE PRECISION,
            prscrptn_drug_pdp_benes                       DOUBLE PRECISION,
            prscrptn_drug_mapd_benes                      DOUBLE PRECISION,
            prscrptn_drug_deemed_eligible_full_lis_benes  DOUBLE PRECISION,
            prscrptn_drug_full_lis_benes                  DOUBLE PRECISION,
            prscrptn_drug_partial_lis_benes               DOUBLE PRECISION,
            prscrptn_drug_no_lis_benes                    DOUBLE PRECISION
        )
        DISTKEY (bene_state_abrvtn)
        SORTKEY (year, month, bene_state_abrvtn);
    """)

    # Full reload — truncate first
    run_sql(f"TRUNCATE TABLE {TARGET_SCHEMA}.{TARGET_TABLE};")

    # COPY from S3
    run_sql(f"""
        COPY {TARGET_SCHEMA}.{TARGET_TABLE}
        FROM '{s3_uri}'
        IAM_ROLE '{iam_role}'
        FORMAT AS PARQUET;
    """)

    logger.info("COPY from S3 complete: %s → %s.%s", s3_uri, TARGET_SCHEMA, TARGET_TABLE)


def validate_mart(**context) -> None:
    """Assert the dbt mart has a reasonable number of national monthly rows."""
    import time

    workgroup = get_var("REDSHIFT_WORKGROUP", "medicare-analytics-workgroup")
    database = get_var("REDSHIFT_DATABASE", "medicare_db")
    region = get_var("AWS_REGION", "us-east-1")

    client = boto3.client("redshift-data", region_name=region)

    response = client.execute_statement(
        WorkgroupName=workgroup,
        Database=database,
        Sql=f"SELECT COUNT(*) FROM {DBT_SCHEMA}.mart_enrollment_national;",
    )
    statement_id = response["Id"]

    while True:
        desc = client.describe_statement(Id=statement_id)
        status = desc["Status"]
        if status == "FINISHED":
            break
        elif status in ("FAILED", "ABORTED"):
            raise RuntimeError(f"Redshift validation query failed: {desc.get('Error')}")
        time.sleep(2)

    result = client.get_statement_result(Id=statement_id)
    row_count = int(result["Records"][0][0]["longValue"])

    min_expected = 100
    assert row_count >= min_expected, (
        f"Mart row count too low: {row_count} — expected at least {min_expected}"
    )
    logger.info("Mart validation passed: %d rows", row_count)


# -----------------------------
# DAG definition
# -----------------------------
with DAG(
    dag_id="medicare_enrollment_pipeline_redshift",
    description="End-to-end Medicare enrollment pipeline: CMS API → S3 → Redshift → dbt",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["medicare", "enrollment", "pipeline", "redshift", "aws"],
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
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
    )

    t4 = PythonOperator(
        task_id="load_s3_to_redshift",
        python_callable=load_s3_to_redshift,
    )

    t5 = BashOperator(
        task_id="run_dbt_transforms",
        bash_command=f"dbt deps --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR} --target prod && dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR} --target prod",
    )

    t6 = PythonOperator(
        task_id="validate_mart",
        python_callable=validate_mart,
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6
