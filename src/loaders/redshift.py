"""
src/loaders/redshift.py

Handles loading data from S3 into Redshift Serverless using the COPY command.

The COPY command is the recommended way to load data into Redshift — it loads
directly from S3 in parallel and is orders of magnitude faster than row-by-row
inserts for large datasets.
"""

from __future__ import annotations

import logging

import boto3

logger = logging.getLogger(__name__)


def get_redshift_data_client(region: str = "us-east-1"):
    """Return a boto3 Redshift Data API client."""
    return boto3.client("redshift-data", region_name=region)


def execute_statement(
    client,
    workgroup_name: str,
    database: str,
    sql: str,
    wait: bool = True,
    poll_interval: int = 2,
) -> dict:
    """
    Execute a SQL statement against Redshift Serverless via the Data API.

    Args:
        client: boto3 redshift-data client.
        workgroup_name: Redshift Serverless workgroup name.
        database: Target database name.
        sql: SQL statement to execute.
        wait: If True, poll until the statement completes.
        poll_interval: Seconds between status polls.

    Returns:
        The final statement description dict.

    Raises:
        RuntimeError: If the statement fails or is aborted.
    """
    import time

    response = client.execute_statement(
        WorkgroupName=workgroup_name,
        Database=database,
        Sql=sql,
    )
    statement_id = response["Id"]
    logger.info("Submitted statement ID: %s", statement_id)

    if not wait:
        return response

    # Poll until terminal state
    while True:
        desc = client.describe_statement(Id=statement_id)
        status = desc["Status"]
        logger.debug("Statement %s status: %s", statement_id, status)

        if status == "FINISHED":
            logger.info("Statement completed successfully")
            return desc
        elif status in ("FAILED", "ABORTED"):
            error = desc.get("Error", "No error details available")
            raise RuntimeError(
                f"Redshift statement {statement_id} {status}: {error}"
            )

        time.sleep(poll_interval)


def create_table_if_not_exists(
    client,
    workgroup_name: str,
    database: str,
    schema: str,
    table: str,
) -> None:
    """
    Create the medicare_monthly_enrollment table in Redshift if it doesn't exist.
    Columns match the CMS API schema with lowercase names.
    Partitioned by bene_enrollmt_ref_yr and bene_enrollmt_ref_mo for query performance.
    """
    sql = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        bene_enrollmt_ref_yr        VARCHAR(4),
        bene_enrollmt_ref_mo        VARCHAR(2),
        bene_geo_lvl                VARCHAR(50),
        bene_state_abrvtn           VARCHAR(2),
        bene_state_cd               VARCHAR(2),
        bene_county_cd              VARCHAR(3),
        bene_fips_cd                VARCHAR(5),
        tot_benes                   BIGINT,
        orgnl_mdcr_benes            BIGINT,
        ma_and_oth_benes            BIGINT,
        aged_tot_benes              BIGINT,
        aged_esrd_benes             BIGINT,
        aged_no_esrd_benes          BIGINT,
        dsbld_tot_benes             BIGINT,
        dsbld_esrd_and_esrd_only_benes BIGINT,
        dsbld_no_esrd_benes         BIGINT,
        male_tot_benes              BIGINT,
        female_tot_benes            BIGINT,
        white_tot_benes             BIGINT,
        black_tot_benes             BIGINT,
        api_tot_benes               BIGINT,
        hspnc_tot_benes             BIGINT,
        natind_tot_benes            BIGINT,
        othr_tot_benes              BIGINT,
        age_lt_25_benes             BIGINT,
        age_25_to_44_benes          BIGINT,
        age_45_to_64_benes          BIGINT,
        age_65_to_69_benes          BIGINT,
        age_70_to_74_benes          BIGINT,
        age_75_to_79_benes          BIGINT,
        age_80_to_84_benes          BIGINT,
        age_85_to_89_benes          BIGINT,
        age_90_to_94_benes          BIGINT,
        age_gt_94_benes             BIGINT,
        dual_tot_benes              BIGINT,
        full_dual_tot_benes         BIGINT,
        part_dual_tot_benes         BIGINT,
        nodual_tot_benes            BIGINT,
        qmb_only_benes              BIGINT,
        qmb_plus_benes              BIGINT,
        slmb_only_benes             BIGINT,
        slmb_plus_benes             BIGINT,
        qdwi_qi_benes               BIGINT,
        othr_full_dual_mdcd_benes   BIGINT,
        a_b_tot_benes               BIGINT,
        a_b_orgnl_mdcr_benes        BIGINT,
        a_b_ma_and_oth_benes        BIGINT,
        a_tot_benes                 BIGINT,
        a_orgnl_mdcr_benes          BIGINT,
        a_ma_and_oth_benes          BIGINT,
        b_tot_benes                 BIGINT,
        b_orgnl_mdcr_benes          BIGINT,
        b_ma_and_oth_benes          BIGINT,
        prscrptn_drug_tot_benes     BIGINT,
        prscrptn_drug_pdp_benes     BIGINT,
        prscrptn_drug_mapd_benes    BIGINT,
        prscrptn_drug_deemed_eligible_full_lis_benes BIGINT,
        prscrptn_drug_full_lis_benes    BIGINT,
        prscrptn_drug_partial_lis_benes BIGINT,
        prscrptn_drug_no_lis_benes      BIGINT
    )
    SORTKEY (bene_enrollmt_ref_yr, bene_enrollmt_ref_mo, bene_state_abrvtn);
    """

    logger.info("Creating table %s.%s if not exists", schema, table)
    execute_statement(client, workgroup_name, database, sql)


def truncate_table(
    client,
    workgroup_name: str,
    database: str,
    schema: str,
    table: str,
) -> None:
    """Truncate the target table before a full reload."""
    sql = f"TRUNCATE TABLE {schema}.{table};"
    logger.info("Truncating table %s.%s", schema, table)
    execute_statement(client, workgroup_name, database, sql)


def copy_from_s3(
    client,
    workgroup_name: str,
    database: str,
    schema: str,
    table: str,
    s3_uri: str,
    iam_role_arn: str,
    region: str = "us-east-1",
) -> None:
    """
    Load data from S3 into Redshift using the COPY command.

    COPY is the recommended bulk load method for Redshift — it reads
    directly from S3 in parallel across Redshift compute nodes.

    Args:
        client: boto3 redshift-data client.
        workgroup_name: Redshift Serverless workgroup name.
        database: Target database name.
        schema: Target schema name.
        table: Target table name.
        s3_uri: S3 URI of the parquet file (s3://bucket/key).
        iam_role_arn: ARN of the IAM role Redshift uses to access S3.
        region: AWS region.
    """
    sql = f"""
    COPY {schema}.{table}
    FROM '{s3_uri}'
    IAM_ROLE '{iam_role_arn}'
    FORMAT AS PARQUET;
    """

    logger.info(
        "Running COPY from %s into %s.%s", s3_uri, schema, table
    )
    execute_statement(client, workgroup_name, database, sql)
    logger.info("COPY complete")


def get_row_count(
    client,
    workgroup_name: str,
    database: str,
    schema: str,
    table: str,
) -> int:
    """Return the current row count of a table."""
    sql = f"SELECT COUNT(*) FROM {schema}.{table};"
    desc = execute_statement(client, workgroup_name, database, sql)

    # Fetch results
    redshift_client = boto3.client("redshift-data")
    result = redshift_client.get_statement_result(Id=desc["Id"])
    count = int(result["Records"][0][0]["longValue"])
    return count
