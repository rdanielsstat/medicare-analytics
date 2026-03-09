# Medicare Enrollment Analytics Pipeline

An end-to-end data engineering project that ingests, transforms, and visualizes U.S. Medicare enrollment data using a modern cloud-native stack. The pipeline runs on AWS with full infrastructure-as-code, orchestrated by Apache Airflow, transformed with dbt, and visualized through an interactive Streamlit dashboard deployed on Streamlit Community Cloud.

Live dashboard: [Medicare Enrollment Dashboard](https://medicare-analytics.streamlit.app/)

---

## Overview

This project builds a production-grade batch data pipeline that processes monthly Medicare enrollment data published by the Centers for Medicare & Medicaid Services (CMS). The data covers enrollment figures for over 65 million beneficiaries across all U.S. states and counties, broken down by plan type, age, sex, and demographic group.

The pipeline ingests raw data from the CMS public API, stages it in Amazon S3, loads it into Amazon Redshift Serverless, transforms it with dbt into analytics-ready mart tables, and exports the results back to S3 for dashboard consumption. The entire cloud infrastructure is provisioned with OpenTofu (open-source Terraform).

---

## Architecture

```
CMS Public API
      |
      v
Apache Airflow (EC2)
      |
      |-- Download & validate raw parquet
      |-- Upload to S3 (data lake)
      |-- Load S3 -> Redshift Serverless (COPY)
      |-- Run dbt transformations
      |-- Validate mart output
      |
      v
Amazon S3 (raw + exports)
      |
      v
Amazon Redshift Serverless (data warehouse)
      |
      v
dbt (staging views + mart tables)
      |
      v
S3 CSV export
      |
      v
Streamlit Dashboard (Community Cloud)
```

### Local Development Architecture

```
CMS Public API
      |
      v
Apache Airflow (Docker)
      |
      v
PostgreSQL (Docker) -- dbt --> dbt_medicare schema
      |
      v
Streamlit (local)
```

---

## Dataset

**Source:** Centers for Medicare & Medicaid Services (CMS) — Monthly Enrollment by Contract/Plan/State/County

**Coverage:** January 2013 through present (monthly updates)

**Volume:** ~557,000 rows per monthly snapshot across all geographic levels

**Geographic levels:** National, State, County

**Key metrics per record:**
- Total beneficiaries
- Original Medicare vs. Medicare Advantage enrollment
- Aged vs. disabled beneficiaries
- Enrollment by sex
- FIPS codes and state/county identifiers

The data dictionary is included at `docs/Medicare Monthly Enrollment Data Dictionary.pdf`.

---

## Tech Stack

| Layer | Local | Cloud |
|--------------------|-------------------------------|----------------------------|
| Orchestration      | Apache Airflow 2.7.2 (Docker) | Apache Airflow 2.7.2 (EC2) |
| Data Lake          | Local filesystem              | Amazon S3                  |
| Data Warehouse     | PostgreSQL 17 (Docker)        | Amazon Redshift Serverless |
| Transformations    | dbt-postgres                  | dbt-redshift               |
| Infrastructure     | Docker Compose                | OpenTofu (IaC)             |
| Dashboard          | Streamlit (local)             | Streamlit Community Cloud  |
| Package Management | uv                            | uv                         |
| CI                 | GitHub Actions                | GitHub Actions             |

---

## Project Structure

```
medicare-analytics/
├── dags/
│   ├── pipelines/
│   │   ├── medicare_enrollment_pipeline_postgres.py   # Local pipeline
│   │   └── medicare_enrollment_pipeline_redshift.py   # AWS pipeline
│   └── exports/
│       └── enrollment_dashboard_export.py             # Export marts to S3
├── src/
│   ├── ingestion/
│   │   └── medicare_enrollment.py                     # CMS API client
│   └── loaders/
│       ├── postgres.py                                # Postgres loader
│       ├── redshift.py                                # Redshift loader
│       └── s3.py                                      # S3 loader
├── medicare_dbt/
│   └── models/
│       ├── staging/
│       │   ├── sources.yml
│       │   └── stg_medicare_enrollment.sql
│       └── marts/
│           ├── mart_enrollment_national.sql
│           └── mart_enrollment_by_state.sql
├── dashboards/
│   ├── app.py
│   └── requirements.txt
├── infra/                                             # OpenTofu IaC
│   ├── main.tf
│   ├── ec2.tf
│   ├── redshift.tf
│   ├── s3.tf
│   ├── iam.tf
│   ├── vpc.tf
│   ├── variables.tf
│   ├── outputs.tf
│   └── terraform.tfvars.example
├── notebooks/                                         # Exploratory analysis
├── .github/
│   └── workflows/
│       └── ci.yml                                     # GitHub Actions CI
├── docker-compose.yml                                 # Local development
├── docker-compose.aws.yml                             # EC2 deployment
├── Makefile
├── .env.example
└── .env.aws.example
```

---

## Data Pipeline

### Local Pipeline (PostgreSQL)

Six-step Airflow DAG (`medicare_enrollment_pipeline_postgres`):

1. `download_from_api` — fetches monthly enrollment data from CMS API, saves as parquet
2. `validate_raw_data` — checks row counts and required columns
3. `upload_to_s3` — uploads parquet to local-equivalent storage
4. `load_to_postgres` — loads parquet into PostgreSQL staging table
5. `run_dbt_transforms` — runs dbt models against postgres (dev target)
6. `validate_mart` — confirms mart table row counts

### Cloud Pipeline (Redshift)

Six-step Airflow DAG (`medicare_enrollment_pipeline_redshift`):

1. `download_from_api` — fetches from CMS API, saves parquet locally on EC2
2. `validate_raw_data` — validates raw data integrity
3. `upload_to_s3` — uploads parquet to S3, skips if already exists
4. `load_s3_to_redshift` — `CREATE TABLE IF NOT EXISTS` + `TRUNCATE` + `COPY FROM S3`
5. `run_dbt_transforms` — runs dbt with Redshift prod target via IAM authentication
6. `validate_mart` — queries `dbt_medicare.mart_enrollment_national` via Redshift Data API

### Dashboard Export Pipeline

Separate DAG (`medicare_dashboard_export`) scheduled to run daily after the main pipeline:

- Queries both mart tables from Redshift via the Data API
- Writes `enrollment_national.csv` and `enrollment_by_state.csv` to S3
- Streamlit reads these CSVs directly — no direct Redshift connection required from the dashboard

---

## dbt Transformations

The dbt project (`medicare_dbt`) uses a two-layer model:

### Staging Layer (`dbt_medicare` schema, materialized as views)

`stg_medicare_enrollment` — cleans and standardizes the raw enrollment table:
- Casts year to integer
- Renames columns to readable names
- Passes through all geographic levels and months for downstream filtering

### Mart Layer (materialized as tables)

`mart_enrollment_national` — one row per month at the national level:
- Filters to `bene_geo_lvl = 'National'` and excludes annual summary rows
- Constructs a `report_date` column compatible with both PostgreSQL and Redshift using `{% if target.type == 'redshift' %}` conditional logic
- Ordered by `report_date`

`mart_enrollment_by_state` — one row per state per year using the CMS annual snapshot:
- Filters to `bene_geo_lvl = 'State'` and `month = 'Year'` (CMS annual totals)
- Includes FIPS codes for choropleth map rendering
- Ordered by year and state abbreviation

### dbt Documentation

Live dbt documentation (lineage graph, model descriptions, column definitions):
https://rdanielsstat.github.io/medicare-analytics/

To regenerate docs locally:
```bash
cd medicare_dbt
dbt docs generate --profiles-dir ../dbt_profiles --target dev
dbt docs serve --profiles-dir ../dbt_profiles --target dev
```

---

## Data Warehouse Design

**Table:** `public.medicare_monthly_enrollment`

**Optimization:** `SORTKEY (year, month, bene_state_abrvtn)`

The sort key is chosen to optimize the most common query patterns: filtering by time period (year/month) and slicing by state. Redshift stores data in 1MB blocks sorted by these columns, which minimizes the number of blocks scanned for time-range and state-based queries.

All numeric columns use `DOUBLE PRECISION` to match the `float64` types in the source parquet files, avoiding casting overhead during COPY.

---

## Infrastructure (OpenTofu / Terraform)

All AWS resources are defined as code in `infra/`:

| Resource | Details |
|---------------------|-----------------------------------------------------------|
| VPC                 | Dedicated VPC with public subnet                          |
| EC2                 | `t3.medium`, Amazon Linux 2023, Elastic IP                |
| S3                  | Single bucket for raw parquet and CSV exports             |
| Redshift Serverless | 32 RPU base capacity, `medicare_db` database              |
| IAM — EC2 role      | S3 read/write, Redshift Data API access                   |
| IAM — Redshift role | S3 read/write for COPY and UNLOAD operations              |
| Security Groups     | SSH restricted to specified CIDR, Airflow UI on port 8080 |

Provision infrastructure:

```bash
cd infra
cp terraform.tfvars.example terraform.tfvars
# Fill in terraform.tfvars with your values
tofu init
tofu plan
tofu apply
```

Tear down:

```bash
tofu destroy
```

---

## CI/CD

GitHub Actions runs on every push and pull request:

**Lint job:**
- Installs `flake8`
- Lints `dags/`, `src/`, and `dashboards/` with max line length 120

**dbt validation job:**
- Spins up a PostgreSQL service container
- Installs `dbt-postgres` and `dbt-redshift`
- Creates a test profiles.yml pointing to the CI postgres instance
- Runs `dbt compile` against the dev target to validate all model SQL
- Runs `dbt test` to execute any defined schema tests

The CI configuration is at `.github/workflows/ci.yml`.

---

## Dashboard

The Streamlit dashboard is deployed at Streamlit Community Cloud and detects its environment automatically:

- **Local:** connects to PostgreSQL via SQLAlchemy, reads from `dbt_medicare` schema
- **Cloud:** reads pre-exported CSV files from S3 via boto3

**Tile 1 — National Enrollment Trends**

A time-series line chart showing total Medicare beneficiaries from 2013 to present, with a toggle to view monthly new beneficiaries as a bar chart. Hover tooltips display formatted enrollment figures.

**Tile 2 — Enrollment by State**

A US choropleth map using Plotly with a year slider. States are shaded by total beneficiary count. Uses FIPS codes and state abbreviations from the mart for accurate geographic rendering.

---

## Local Setup

### Prerequisites

- Docker Desktop
- Python 3.12+
- `uv` package manager (`pip install uv`)
- AWS CLI (for cloud deployment only)
- OpenTofu (for infrastructure provisioning only)

### 1. Clone the repository

```bash
git clone https://github.com/rdanielsstat/medicare-analytics.git
cd medicare-analytics
```

### 2. Install dependencies

```bash
uv sync
```

### 3. Configure environment variables

```bash
cp .env.example .env
# Edit .env with your values
```

Required variables:

```
POSTGRES_USER=your_db_user
POSTGRES_PASSWORD=your_db_password
POSTGRES_DB=medicare_db
POSTGRES_PORT=5432
PGADMIN_DEFAULT_EMAIL=your_email@example.com
PGADMIN_DEFAULT_PASSWORD=your_pgadmin_password
AIRFLOW_UID=50000
AIRFLOW_FERNET_KEY=<generate with: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())">
```

### 4. Start services

```bash
make up
# or
docker compose up -d
```

This starts PostgreSQL, Airflow (scheduler + webserver), and pgAdmin.

### 5. Access Airflow

Navigate to `http://localhost:8080` — username `admin`, password `admin`.

### 6. Configure dbt profiles

```bash
cp dbt_profiles/profiles.yml.example dbt_profiles/profiles.yml
# Edit with your local postgres credentials
```

### 7. Trigger the pipeline

In the Airflow UI, trigger `medicare_enrollment_pipeline_postgres` with parameter:

```json
{"release_month": "2025-11"}
```

### 8. Run the dashboard locally

```bash
cd dashboards
streamlit run app.py
```

### Makefile commands

```bash
make up        # Start all services
make down      # Stop all services
make logs      # Tail Airflow logs
make restart   # Restart services
```

---

## Cloud Deployment (AWS)

### Prerequisites

- AWS account with CLI configured
- OpenTofu installed
- EC2 key pair created in `us-east-1`

### 1. Provision infrastructure

```bash
cd infra
cp terraform.tfvars.example terraform.tfvars
# Fill in your values including key pair name and home IP CIDR
tofu init && tofu apply
```

Note the outputs — you will need the EC2 public IP and Redshift endpoint.

### 2. Configure EC2

SSH into the instance:

```bash
ssh -i ~/.ssh/your-key.pem ec2-user@<ec2-public-ip>
```

Clone the repository:

```bash
git clone https://github.com/rdanielsstat/medicare-analytics.git
cd medicare-analytics
git remote set-url --push origin no_push   # EC2 is read-only
```

Set up environment:

```bash
cp .env.aws.example .env
# Edit .env with your Redshift endpoint, S3 bucket, and other values
```

Set up dbt profiles (not committed to git):

```bash
mkdir -p dbt_profiles
# Create dbt_profiles/profiles.yml — see .env.aws.example for required variables
```

Fix permissions and start services:

```bash
sudo chown -R 50000:0 logs data/raw/enrollment medicare_dbt
docker compose -f docker-compose.aws.yml up -d
```

### 3. Set Airflow variables

In the Airflow UI (`http://<ec2-ip>:8080`), set these under Admin → Variables:

- `S3_BUCKET`
- `REDSHIFT_WORKGROUP`
- `REDSHIFT_DATABASE`
- `REDSHIFT_IAM_ROLE`
- `AWS_REGION`

### 4. Trigger the pipeline

Trigger `medicare_enrollment_pipeline_redshift` with `{"release_month": "2025-11"}`, then trigger `medicare_dashboard_export`.

### 5. Stopping to avoid costs

```bash
docker compose -f docker-compose.aws.yml down
exit
```

Stop the EC2 instance from the AWS console. Redshift Serverless pauses automatically when idle. Estimated idle cost: ~$0.15/day (Elastic IP + EBS volume).

---

## Environment Variable Handling

Secrets are never committed to git. The pattern used throughout:

- `.env.example` and `.env.aws.example` — committed templates with placeholder values
- `.env` — gitignored, created manually on each machine
- `dbt_profiles/profiles.yml` — gitignored, created manually on EC2
- `infra/terraform.tfvars` — gitignored, created manually
- Streamlit secrets — configured via Streamlit Community Cloud UI

---

## Notebooks

Exploratory notebooks are in `notebooks/`:

- `01_explore_enrollment_data.ipynb` — initial data exploration of the CMS dataset
- `02_query_postgres.ipynb` — querying the local postgres warehouse
- `03_app_testing.ipynb` — prototyping dashboard queries

---

## Logging

Airflow task logs are written to `logs/` (gitignored, mounted as a Docker volume). Each task run produces a structured log file at:

```
logs/dag_id=<dag>/run_id=<run>/task_id=<task>/attempt=<n>.log
```

Application-level logging uses Python's standard `logging` module with `INFO` level throughout the DAG and loader modules.
