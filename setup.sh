#!/bin/bash
set -e
mkdir -p logs medicare_dbt/logs data
sudo chown -R 50000:0 logs medicare_dbt data
docker compose up -d