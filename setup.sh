#!/bin/bash
set -e

# Create dirs the containers write into
mkdir -p logs data/raw/enrollment medicare_dbt/logs medicare_dbt/target

# Airflow (UID 50000) needs its log dir; dbt needs its own log/target dirs.
# Chown medicare_dbt root (so dbt can write package-lock.yml) but exclude .git
sudo chown -R 50000:0 logs data medicare_dbt/logs medicare_dbt/target medicare_dbt
sudo chown -R ec2-user:ec2-user medicare_dbt/.git 2>/dev/null || true

docker compose up -d