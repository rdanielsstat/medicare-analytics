"""
medicare_enrollment.py

Download Medicare Monthly Enrollment data from CMS API and save it locally with
a timestamped filename.

Overview:
    https://data.cms.gov/summary-statistics-on-beneficiary-enrollment/medicare-and-medicaid-reports/medicare-monthly-enrollment

API documentation:
    https://data.cms.gov/summary-statistics-on-beneficiary-enrollment/medicare-and-medicaid-reports/medicare-monthly-enrollment/api-docs

API currently caps at 5,000 rows per request

Usage:
    python medicare_enrollment.py --release-month 2025-11
    python medicare_enrollment.py --release-month 2025-12 --force  # re-download even if file exists
    python medicare_enrollment.py --release-month 2025-12 --page-size 1000
"""

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# -----------------------------
# Constants
# -----------------------------
API_URL = "https://data.cms.gov/data-api/v1/dataset/d7fabe1e-d19b-4333-9eff-e80e0643f2fd/data"
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

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
# HTTP Session with retry logic
# -----------------------------
def build_session(
    total_retries: int = 5,
    backoff_factor: float = 1.0,
    status_forcelist: tuple = (429, 500, 502, 503, 504),
) -> requests.Session:
    """Return a requests.Session with automatic retries and exponential backoff."""
    session = requests.Session()
    retry = Retry(
        total=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


# -----------------------------
# Download
# -----------------------------
def download_data(
    session: requests.Session,
    page_size: int = 5000,
    max_pages: int = 500,
) -> list[dict]:
    """
    Paginate through the CMS API and return all records as a list of dicts.

    Raises RuntimeError if max_pages is reached (signals unexpected API behavior).
    """
    all_rows: list[dict] = []
    offset = 0

    for page_num in range(max_pages):
        logger.info("Fetching rows %d – %d  (page %d)…", offset, offset + page_size - 1, page_num + 1)

        response = session.get(API_URL, params={"offset": offset, "size": page_size}, timeout=60)
        response.raise_for_status()

        page_data: list[dict] = response.json()
        n_rows = len(page_data)

        if n_rows == 0:
            logger.info("Empty page received — pagination complete.")
            break

        all_rows.extend(page_data)
        offset += n_rows
        logger.info("  → %d rows on this page  |  %d total so far", n_rows, len(all_rows))
    else:
        raise RuntimeError(
            f"Reached max_pages limit ({max_pages}). "
            "The API may be returning unexpected results — inspect and increase max_pages if needed."
        )

    return all_rows


# -----------------------------
# Transform
# -----------------------------
def build_dataframe(records: list[dict]) -> pd.DataFrame:
    """Convert raw API records to a typed DataFrame."""
    df = pd.DataFrame(records)

    # Cast known numeric columns; coerce bad values to NaN
    cols_to_cast = [c for c in NUMERIC_COLS if c in df.columns]
    df[cols_to_cast] = df[cols_to_cast].apply(pd.to_numeric, errors="coerce")

    # Warn about any expected columns that are absent (schema drift detection)
    missing_cols = set(NUMERIC_COLS) - set(df.columns)
    if missing_cols:
        logger.warning(
            "The following expected columns were NOT found in the API response "
            "(possible schema change): %s",
            sorted(missing_cols),
        )

    return df


# -----------------------------
# Main
# -----------------------------
def main(release_month: str, page_size: int, force: bool) -> None:
    # Resolve paths
    data_dir = PROJECT_ROOT / "data" / "raw" / "enrollment" / release_month
    data_dir.mkdir(parents=True, exist_ok=True)
    out_path = data_dir / f"medicare_monthly_enrollment_{release_month}.parquet"

    # Skip if already downloaded (unless --force)
    if out_path.exists() and not force:
        logger.info("File already exists at %s — skipping download. Use --force to re-download.", out_path)
        sys.exit(0)

    logger.info("Starting download for release month: %s", release_month)

    session = build_session()
    records = download_data(session, page_size=page_size)
    logger.info("Download complete: %d total rows", len(records))

    df = build_dataframe(records)
    logger.info("DataFrame shape: %s", df.shape)

    df.to_parquet(out_path, engine="pyarrow", index=False, compression="snappy")
    logger.info("Saved → %s", out_path)


# -----------------------------
# Entry point
# -----------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download CMS Medicare Monthly Enrollment data to Parquet.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--release-month",
        required=True,
        metavar="YYYY-MM",
        help="Release month to download, e.g. 2025-11",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=5000,
        metavar="N",
        help="Number of rows to request per API page",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download even if the output file already exists",
    )

    args = parser.parse_args()
    main(
        release_month=args.release_month,
        page_size=args.page_size,
        force=args.force,
    )
