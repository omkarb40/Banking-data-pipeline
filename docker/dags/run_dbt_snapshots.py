"""
DAG: dbt Transformations & Snapshots
======================================
Runs daily at midnight UTC:
  1. dbt snapshot  (SCD Type-2 history capture)
  2. dbt run       (staging → marts)
  3. dbt test      (data quality validation)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "banking-pipeline",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Path to dbt project inside the container
DBT_PROJECT_DIR = "/opt/airflow/dbt/banking_dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"

# Common dbt command prefix
DBT_CMD = f"cd {DBT_PROJECT_DIR} && dbt"
DBT_FLAGS = f"--profiles-dir {DBT_PROFILES_DIR}"

with DAG(
    dag_id="dbt_transformations",
    default_args=default_args,
    description="Run dbt snapshots, models, and tests",
    schedule_interval="0 0 * * *",  # midnight UTC daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["banking", "dbt", "transformations"],
) as dag:

    # ── Step 1: dbt deps (install packages) ──────────────────
    t_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"{DBT_CMD} deps {DBT_FLAGS}",
    )

    # ── Step 2: dbt snapshot (SCD Type-2) ────────────────────
    t_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"{DBT_CMD} snapshot {DBT_FLAGS}",
    )

    # ── Step 3: dbt run — staging models ─────────────────────
    t_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"{DBT_CMD} run --select staging {DBT_FLAGS}",
    )

    # ── Step 4: dbt run — mart models ────────────────────────
    t_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"{DBT_CMD} run --select marts {DBT_FLAGS}",
    )

    # ── Step 5: dbt test — data quality ──────────────────────
    t_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT_CMD} test {DBT_FLAGS}",
    )

    # ── Step 6: dbt docs generate ────────────────────────────
    t_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"{DBT_CMD} docs generate {DBT_FLAGS}",
    )

    t_deps >> t_snapshot >> t_run_staging >> t_run_marts >> t_test >> t_docs
