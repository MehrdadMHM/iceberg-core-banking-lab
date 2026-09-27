# Iceberg Core Banking Lab

Local core banking data lab using PostgreSQL, Spark, Apache Iceberg and dbt.

## Data flow

PostgreSQL source tables → Spark JDBC load → Iceberg bronze/history tables → dbt silver, Raw Vault, Business Vault and gold models.

The Business Vault PIT models retain customer and account change points. The account example records ACTIVE → INACTIVE.

## Local prerequisites

- Docker Desktop and Docker Compose
- An existing PostgreSQL service reachable as `postgres` on the external Docker network `airflow_default`
- PostgreSQL JDBC driver `postgresql-42.7.13.jar` in `jars/`
- Local environment variables `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER` and `PGPASSWORD` for the Python loaders

The `warehouse/` data, JDBC jar, local dbt `profiles.yml`, and notebook outputs are excluded from Git. Copy `dbt_project/profiles.example.yml` to `dbt_project/profiles.yml` and set the local Spark Thrift host and user before running dbt.

## Validation

`dbt build` completed with 28 PASS, 0 WARN and 0 ERROR on 2026-09-27.