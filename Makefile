# ─────────────────────────────────────────
# Medicare Analytics — Makefile
# ─────────────────────────────────────────

# ── Docker ───────────────────────────────
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

logs:
	docker compose logs -f

# ── DAG triggers ─────────────────────────
month ?= 2025-12

trigger:
	docker exec -it airflow_webserver airflow dags trigger medicare_enrollment_pipeline_postgres \
		--conf '{"release_month": "$(month)"}'

trigger-force:
	docker exec -it airflow_webserver airflow dags trigger medicare_enrollment_pipeline_postgres \
		--conf '{"release_month": "$(month)", "force": true}'

# ── dbt ──────────────────────────────────
dbt-run:
	cd medicare_dbt && dbt run --profiles-dir ../dbt_profiles --target local

dbt-test:
	cd medicare_dbt && dbt test --profiles-dir ../dbt_profiles --target local

dbt-docs:
	cd medicare_dbt && dbt docs generate --profiles-dir ../dbt_profiles --target local && dbt docs serve --profiles-dir ../dbt_profiles --target local
	
# ── Dashboard ────────────────────────────
streamlit:
	uv run streamlit run dashboards/app.py

# ── Tools ────────────────────────────────
pgadmin:
	open http://localhost:8085

airflow:
	open http://localhost:8080