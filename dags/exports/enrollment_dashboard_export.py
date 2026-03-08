from datetime import datetime, timedelta
import logging
import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

logger = logging.getLogger(__name__)

def get_var(key, default=None):
    try:
        return Variable.get(key)
    except Exception:
        import os
        return os.getenv(key, default)

def query_to_dataframe(client, workgroup: str, database: str, sql: str) -> pd.DataFrame:
    """Execute a Redshift Data API query and return results as a DataFrame."""
    import time

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
            raise RuntimeError(f"Query failed: {desc.get('Error')}")
        time.sleep(2)

    results = client.get_statement_result(Id=statement_id)
    columns = [col["name"] for col in results["ColumnMetadata"]]
    rows = [
        [field.get("stringValue") or field.get("doubleValue") or field.get("longValue")
         for field in record]
        for record in results["Records"]
    ]
    return pd.DataFrame(rows, columns=columns)


def upload_to_s3(s3_client, df: pd.DataFrame, bucket: str, key: str) -> None:
    """Upload a DataFrame to S3 as CSV."""
    import io
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="text/csv",
    )


def export_marts_to_s3(**context) -> None:
    """Export all dashboard marts from Redshift to S3 as CSV."""
    import boto3

    workgroup = get_var("REDSHIFT_WORKGROUP", "medicare-analytics-workgroup")
    region = get_var("AWS_REGION", "us-east-1")
    bucket = get_var("S3_BUCKET", "medicare-analytics-raw-rdaniels")

    redshift = boto3.client("redshift-data", region_name=region)
    s3 = boto3.client("s3", region_name=region)

    exports = [
        {
            "sql": "SELECT * FROM dbt_medicare.mart_enrollment_national ORDER BY report_date",
            "key": "exports/mart_enrollment_national/enrollment_national.csv",
            "description": "national enrollment",
        },
        {
            "sql": "SELECT * FROM dbt_medicare.mart_enrollment_by_state ORDER BY year, state",
            "key": "exports/mart_enrollment_by_state/enrollment_by_state.csv",
            "description": "enrollment by state",
        },
    ]

    for export in exports:
        df = query_to_dataframe(redshift, workgroup, "medicare_db", export["sql"])
        upload_to_s3(s3, df, bucket, export["key"])
        logger.info(f"Exported {len(df)} rows → s3://{bucket}/{export['key']}")

with DAG(
    dag_id="medicare_dashboard_export",
    description="Export Medicare mart tables from Redshift to S3 for dashboard consumption",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 6 * * *",  # daily at 6am UTC, after main pipeline
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["medicare", "export", "dashboard"],
) as dag:

    t1 = PythonOperator(
        task_id="export_marts_to_s3",
        python_callable=export_marts_to_s3,
    )