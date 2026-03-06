"""
src/loaders/s3.py

Handles uploading local parquet files to S3.
"""

from __future__ import annotations

import logging
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


def upload_parquet_to_s3(
    local_path: Path | str,
    bucket: str,
    s3_key: str | None = None,
) -> str:
    """
    Upload a local parquet file to S3.

    Args:
        local_path: Path to the local parquet file.
        bucket: S3 bucket name.
        s3_key: S3 object key (path inside bucket). If None, mirrors the
                local path structure starting from 'enrollment/'.

    Returns:
        The full S3 URI of the uploaded file (s3://bucket/key).

    Raises:
        FileNotFoundError: If the local file does not exist.
        ClientError: If the S3 upload fails.
    """
    local_path = Path(local_path)

    if not local_path.exists():
        raise FileNotFoundError(f"Local file not found: {local_path}")

    # Mirror local folder structure inside the bucket if no key provided
    # e.g. data/raw/enrollment/2025-11/medicare_monthly_enrollment_2025-11.parquet
    #   -> raw/enrollment/2025-11/medicare_monthly_enrollment_2025-11.parquet
    if s3_key is None:
        # Find 'enrollment' in the path and use everything from there
        parts = local_path.parts
        try:
            enrollment_idx = parts.index("enrollment")
            s3_key = "/".join(parts[enrollment_idx:])
        except ValueError:
            # Fallback: just use the filename
            s3_key = f"enrollment/{local_path.name}"

    s3_client = boto3.client("s3")

    logger.info("Uploading %s to s3://%s/%s", local_path, bucket, s3_key)

    try:
        s3_client.upload_file(
            Filename=str(local_path),
            Bucket=bucket,
            Key=s3_key,
        )
    except ClientError as e:
        logger.error("Failed to upload to S3: %s", e)
        raise

    s3_uri = f"s3://{bucket}/{s3_key}"
    logger.info("Upload complete: %s", s3_uri)
    return s3_uri


def get_s3_uri(bucket: str, s3_key: str) -> str:
    """Return the S3 URI for a given bucket and key."""
    return f"s3://{bucket}/{s3_key}"


def file_exists_in_s3(bucket: str, s3_key: str) -> bool:
    """Check if a file already exists in S3."""
    s3_client = boto3.client("s3")
    try:
        s3_client.head_object(Bucket=bucket, Key=s3_key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise
