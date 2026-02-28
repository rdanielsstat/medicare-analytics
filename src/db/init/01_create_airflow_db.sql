-- init-db/01_create_airflow_db.sql
-- This script runs automatically on first initialization of a fresh postgres
-- volume via docker-entrypoint-initdb.d. It will not run on subsequent starts.
-- It creates the dedicated Airflow metadata database so it stays separate
-- from the Medicare analytics database.
CREATE DATABASE airflow OWNER medicare_user ;