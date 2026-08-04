#!/bin/bash
set -e

# Create dirs the containers write into
mkdir -p logs data medicare_dbt/logs medicare_dbt/target

# Airflow (UID 50000) needs its log dir; dbt needs its own log/target dirs.
# Only chown the WRITABLE output dirs, never the whole medicare_dbt tree,
# or git pull can't overwrite tracked files (owned by ec2-user).
sudo chown -R 50000:0 logs data medicare_dbt/logs medicare_dbt/target

docker compose up -d