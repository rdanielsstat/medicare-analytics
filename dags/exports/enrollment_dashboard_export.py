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

def export_mart_to_s3(**context) -> None:
    """Export mart_enrollment_national from Redshift to S3 as CSV for dashboard."""
    import time
    import boto3
    import pandas as pd
    import io

    workgroup = get_var("REDSHIFT_WORKGROUP", "medicare-analytics-workgroup")
    region = get_var("AWS_REGION", "us-east-1")
    bucket = get_var("S3_BUCKET", "medicare-analytics-raw-rdaniels")

    # Query Redshift via Data API
    client = boto3.client("redshift-data", region_name=region)
    response = client.execute_statement(
        WorkgroupName=workgroup,
        Database="medicare_db",
        Sql="SELECT * FROM dbt_medicare.mart_enrollment_national ORDER BY report_date",
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

    # Fetch results and convert to dataframe
    results = client.get_statement_result(Id=statement_id)
    columns = [col["name"] for col in results["ColumnMetadata"]]
    rows = [[field.get("stringValue") or field.get("doubleValue") or field.get("longValue") 
             for field in record] for record in results["Records"]]
    df = pd.DataFrame(rows, columns=columns)

    # Upload to S3 as CSV
    s3 = boto3.client("s3", region_name=region)
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    s3.put_object(
        Bucket=bucket,
        Key="exports/mart_enrollment_national/enrollment_national.csv",
        Body=buffer.getvalue(),
        ContentType="text/csv",
    )

    logger.info(f"Exported {len(df)} rows to s3://{bucket}/exports/mart_enrollment_national/enrollment_national.csv")


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
        task_id="export_mart_to_s3",
        python_callable=export_mart_to_s3,
    )