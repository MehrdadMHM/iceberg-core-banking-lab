"""Manual orchestration for the local PostgreSQL -> Iceberg -> dbt lab.

Install at C:\\AirFlow\\dags\\core_banking_iceberg_pipeline.py.
Requires Docker CLI/socket in Airflow, running spark-iceberg and dbt-iceberg,
and an Airflow connection named iceberg_source_postgres.
"""

import os
import subprocess
from datetime import datetime, timezone
from airflow.sdk import dag, task
from airflow.hooks.base import BaseHook



DBT_ARGS = ("--project-dir", "/workspace", "--profiles-dir", "/workspace")


def docker_exec(container, *command, pg_credentials=False):
    args = ["docker", "exec"]
    env = os.environ.copy()

    if pg_credentials:
        connection = BaseHook.get_connection("iceberg_source_postgres")
        if not connection.login or not connection.password:
            raise ValueError("Airflow connection iceberg_source_postgres needs login/password")
        env["PGUSER"] = connection.login
        env["PGPASSWORD"] = connection.password
        args += ["-e", "PGUSER", "-e", "PGPASSWORD"]

    args += [container, *command]
    subprocess.run(args, env=env, check=True)


def run_dbt(model_path):
    docker_exec(
        "dbt-iceberg", "dbt", "run", "--select", model_path, *DBT_ARGS
    )


@dag(
    dag_id="core_banking_iceberg_pipeline",
    start_date=datetime(2026, 9, 26, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["core-banking", "iceberg", "dbt", "data-vault"],
)
def core_banking_iceberg_pipeline():
    @task(retries=0)
    def load_current_bronze():
        docker_exec(
            "spark-iceberg", "spark-submit", "/home/iceberg/load_bronze.py",
            pg_credentials=True,
        )

    @task(retries=0)
    def build_silver():
        run_dbt("path:models/silver")

    @task(retries=0)
    def build_gold():
        run_dbt("path:models/gold")

    @task(retries=0)
    def append_bronze_history():
        # Keep retries disabled until the snapshot loader has a durable run-id guard.
        docker_exec(
            "spark-iceberg", "spark-submit", "/home/iceberg/load_bronze_history.py",
            pg_credentials=True,
        )

    @task(retries=0)
    def build_raw_vault():
        run_dbt("path:models/raw_vault")

    @task(retries=0)
    def build_business_vault():
        run_dbt("path:models/business_vault")

    current = load_current_bronze()
    silver = build_silver()
    gold = build_gold()
    history = append_bronze_history()
    raw = build_raw_vault()
    business = build_business_vault()

    current >> silver >> gold >> history >> raw >> business


core_banking_iceberg_pipeline()
