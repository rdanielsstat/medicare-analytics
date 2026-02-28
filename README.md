# Architecture (Industry-Realistic)

## Cloud

Amazon Web Services (AWS)

Reasons:
- Most widely used cloud for data engineering jobs
- Huge ecosystem
- Free tier works for this project

Key AWS components:

- S3 → Data lake
- EC2 → Airflow / Spark compute
- IAM → permissions
- Glue Catalog (optional but good signal)

## Data Pipeline Design

Do batch for this project. Streaming adds complexity but not much value for CMS healthcare data. A daily pipeline is perfect.

Example flow:

```text
CMS Dataset API / CSV
        ↓
Airflow DAG (scheduled daily)
        ↓
Raw Data → S3 (Data Lake)
        ↓
Spark Processing
        ↓
Clean Data → S3 (Parquet)
        ↓
dbt transformations
        ↓
Data Warehouse
        ↓
Streamlit dashboard
```

## Tools (Strong Resume Stack)

### Orchestration

Apache Airflow

Why:
- Most widely used orchestration framework
- Shows real pipeline skills
- Companies expect it

Run it in Docker on EC2. DAG should include:

```text
download_data
upload_to_s3
spark_transform
load_to_warehouse
run_dbt_models
```

This easily earns full pipeline points.

### Data Lake

Use Amazon S3. Structure it like a real lake:

```text
s3://cms-data-lake/

raw/
    provider_costs/
        year=2024/
processed/
    provider_costs/
        parquet/
analytics/
```

Store processed data in Parquet. Employers love seeing columnar formats.

### Batch Processing

Use Apache Spark. Spark is still the most requested DE skill. Run it via:

```text
PySpark + Docker container
```

or

```text
spark-submit on EC2
```

Processing tasks:
- clean CMS dataset
- normalize columns
- convert CSV → Parquet
- partition by date or state

### Data Warehouse

Best choice:

PostgreSQL

Reasons:
- Open source
- Free
- Works well with dbt
- Easy to run in Docker
- Recruiters recognize it

Alternative:

Amazon Redshift

May cost money. Postgres is perfectly acceptable for a portfolio project.

### Transformations

Use `dbt`. This is extremely important for employability.

`dbt` shows:
- analytics engineering
- modeling
- testing
- documentation

`dbt` models might include:

```text
stg_providers
stg_costs
fact_utilization
dim_provider
```

Also include:
- `dbt` tests
- `dbt` docs

### Dashboard

Best option:

Streamlit

Reasons:
- Python
- Easy deployment
- widely used in portfolios

Dashboard could show:

Tile 1 (categorical distribution)

```text
Utilization by Provider Type
```

Tile 2 (time series)

```text
Total Medicare Cost by Year
```

Bonus:

```text
State-level cost heatmap
```

### Containerization

Use Docker. Containerize:

```
Airflow
Spark
Postgres
dbt
```

A docker-compose setup is perfect. Employers expect Docker now.

### CI/CD (very strong signal)

Use GitHub Actions. Pipeline should run:

```text
lint
pytest
dbt test
build docker images
```

Example:

```text
on push → run tests
```

### Testing

Add:

pytest

Tests for:
- data ingestion
- schema validation
- transformations

Example:

```text
tests/test_ingestion.py
tests/test_transformations.py
```

`dbt` also supports built-in tests.

### Infrastructure as Code

This is a huge bonus for employers.

Use:

Terraform

Provision:

```text
S3 bucket
EC2 instance
IAM roles
```

This would give full marks for the cloud category.

### Makefile

Use Make to simplify commands.

Example:

```text
make start-airflow
make run-pipeline
make dbt-run
make tests
```

Employers love seeing this.

## Final Stack Summary (Very Strong Resume)
| Layer           | Tool           |
|-----------------|----------------|
| Cloud           | AWS            |
| Data Lake       | S3             |
| Orchestration   | Airflow        |
| Processing      | Spark          |
| Warehouse       | PostgreSQL     |
| Transformations | dbt            |
| Dashboard       | Streamlit      |
| Containerization| Docker         |
| CI/CD           | GitHub Actions |
| IaC             | Terraform      |
| Testing         | pytest         |

This is extremely close to a real production stack.

## Repo

```text
cms-provider-analytics/
docker/
airflow/
spark/
dbt/
dashboard/
terraform/
tests/
Makefile
docker-compose.yml
README.md
```

## Project Idea

"Medicare Provider Cost & Utilization Analytics"

Problem:

> Healthcare spending is difficult to analyze across providers and geography. This project builds a scalable pipeline that ingests CMS provider data and enables interactive analytics on cost and utilization trends.

Dashboard insights:
- Cost by provider type
- Yearly utilization trend
- State-level spending

### One Important Tip

Do not overcomplicate the dataset.

Employers care more about:
- architecture
- clean code
- reproducibility

than massive data.