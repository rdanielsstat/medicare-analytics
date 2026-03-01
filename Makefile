# ─────────────────────────────────────────
# Medicare Analytics — Makefile
# ─────────────────────────────────────────

# Docker
up:
	docker compose up -d

down:
	docker compose down

down-v:
	docker compose down -v

restart:
	docker compose down && docker compose up -d

rebuild:
	docker compose down --rmi all && docker compose build --no-cache && docker compose up -d

# Airflow
create-user:
	docker exec -it airflow_webserver airflow users create \
		--username admin \
		--password admin \
		--firstname Admin \
		--lastname User \
		--role Admin \
		--email admin@example.com

list-users:
	docker exec -it airflow_webserver airflow users list

add-connection:
	docker exec -it airflow_webserver airflow connections add medicare_postgres \
		--conn-type postgres \
		--conn-host postgres \
		--conn-login medicare_user \
		--conn-password medicare_pass \
		--conn-schema medicare_db \
		--conn-port 5432

# DAG triggers
month ?= 2025-11

trigger:
	docker exec -it airflow_webserver airflow dags trigger medicare_enrollment_pipeline_postgres \
		--conf '{"release_month": "$(month)"}'

trigger-force:
	docker exec -it airflow_webserver airflow dags trigger medicare_enrollment_pipeline_postgres \
		--conf '{"release_month": "$(month)", "force": true}'

# {"release_month": "2025-11", "force": true, "page_size": 5000}

delete-old-dag:
	docker exec -it airflow_webserver airflow dags delete medicare_enrollment_load

# Database
row-count:
	docker exec -it medicare_postgres psql -U medicare_user -d medicare_db \
		-c "SELECT COUNT(*) FROM medicare_monthly_enrollment;"

# dbt
dbt-run:
	cd medicare_dbt && dbt run

dbt-test:
	cd medicare_dbt && dbt test

# Streamlit
streamlit:
	uv run streamlit run dashboards/app.py

# PGAdmin
pgadmin:
	open http://localhost:8085