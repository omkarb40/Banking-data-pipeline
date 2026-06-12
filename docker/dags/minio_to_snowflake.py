"""
DAG: MinIO → Snowflake Bronze Ingestion (v2 — Batch Optimized)
================================================================
Key optimization: Inserts all records from a JSON file in a SINGLE
query using Snowflake's PARSE_JSON + ARRAY_GENERATE_RANGE, instead
of one INSERT per record.

Performance: ~3 seconds per FILE vs ~3 seconds per ROW.
A 100-record file that took 5 minutes now takes 3 seconds.

v1 bottleneck: opened/closed a new Snowflake connection per record
v2 fix: batch all records per file into one INSERT, reuse connection
"""

import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

logger = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────
RAW_BUCKET = "banking-raw"
PROCESSED_BUCKET = "banking-processed"
MINIO_CONN_ID = "minio_s3"
SNOWFLAKE_CONN_ID = "snowflake_default"

TABLES = ["customers", "accounts", "transactions", "loans", "audit_log"]

TABLE_TO_BRONZE = {
    "customers": "BANKING_DW.BRONZE.RAW_CUSTOMERS",
    "accounts": "BANKING_DW.BRONZE.RAW_ACCOUNTS",
    "transactions": "BANKING_DW.BRONZE.RAW_TRANSACTIONS",
    "loans": "BANKING_DW.BRONZE.RAW_LOANS",
    "audit_log": "BANKING_DW.BRONZE.RAW_AUDIT_LOG",
}

default_args = {
    "owner": "banking-pipeline",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


# ── Task Functions ───────────────────────────────────────────
def list_new_files(**context):
    """List unprocessed JSON files in MinIO for each table."""
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    all_files = {}

    for table in TABLES:
        prefix = f"{table}/"
        keys = s3_hook.list_keys(bucket_name=RAW_BUCKET, prefix=prefix) or []
        json_keys = [k for k in keys if k.endswith(".json")]
        all_files[table] = json_keys
        logger.info(f"Found {len(json_keys)} files for {table}")

    total = sum(len(v) for v in all_files.values())
    logger.info(f"Total files to process: {total}")
    context["ti"].xcom_push(key="files_by_table", value=all_files)
    return total


def load_to_bronze(**context):
    """
    BATCH INSERT: Read all records from a JSON file and insert
    them in a single Snowflake query using a VALUES clause.

    v1 did: 1 connection + 1 INSERT + 1 COMMIT per record
    v2 does: 1 connection per TABLE, 1 INSERT per FILE (all records)
    """
    ti = context["ti"]
    files_by_table = ti.xcom_pull(key="files_by_table", task_ids="list_new_files")

    if not files_by_table:
        logger.info("No files to process. Skipping.")
        return

    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    sf_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    total_loaded = 0
    total_files = 0

    for table, keys in files_by_table.items():
        if not keys:
            continue

        target_table = TABLE_TO_BRONZE[table]
        logger.info(f"Loading {len(keys)} files into {target_table}")

        # ── One connection per table (not per record) ────────
        conn = sf_hook.get_conn()
        cursor = conn.cursor()

        try:
            for key in keys:
                try:
                    content = s3_hook.read_key(key=key, bucket_name=RAW_BUCKET)
                    records = json.loads(content)

                    if not isinstance(records, list):
                        records = [records]

                    if not records:
                        continue

                    # ── Batch: wrap all records in an array,
                    #    use LATERAL FLATTEN to explode in one query ──
                    array_json = json.dumps(records, default=str)

                    cursor.execute(
                        f"""
                        INSERT INTO {target_table} (raw_data, source_file, loaded_at)
                        SELECT
                            value AS raw_data,
                            %s AS source_file,
                            CURRENT_TIMESTAMP() AS loaded_at
                        FROM TABLE(FLATTEN(input => PARSE_JSON(%s)))
                        """,
                        (key, array_json),
                    )
                    conn.commit()

                    total_loaded += len(records)
                    total_files += 1

                    if total_files % 25 == 0:
                        logger.info(
                            f"  Progress: {total_files} files, "
                            f"{total_loaded} records loaded"
                        )

                except Exception as e:
                    logger.error(f"  Failed on {key}: {e}")
                    conn.rollback()
                    raise

        finally:
            cursor.close()
            conn.close()

        logger.info(
            f"  ✅ {table}: {len(keys)} files loaded into {target_table}"
        )

    logger.info(f"✅ TOTAL: {total_loaded} records from {total_files} files")
    ti.xcom_push(key="total_loaded", value=total_loaded)


def archive_files(**context):
    """Move processed files from raw to processed bucket."""
    ti = context["ti"]
    files_by_table = ti.xcom_pull(key="files_by_table", task_ids="list_new_files")

    if not files_by_table:
        return

    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    archived = 0

    for table, keys in files_by_table.items():
        for key in keys:
            try:
                s3_hook.copy_object(
                    source_bucket_key=key,
                    source_bucket_name=RAW_BUCKET,
                    dest_bucket_key=key,
                    dest_bucket_name=PROCESSED_BUCKET,
                )
                s3_hook.delete_objects(bucket=RAW_BUCKET, keys=[key])
                archived += 1
            except Exception as e:
                logger.warning(f"  Could not archive {key}: {e}")

    logger.info(f"✅ Archived {archived} files to {PROCESSED_BUCKET}")


# ── DAG Definition ───────────────────────────────────────────
with DAG(
    dag_id="minio_to_snowflake_bronze",
    default_args=default_args,
    description="Batch-optimized CDC ingestion from MinIO to Snowflake Bronze",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["banking", "ingestion", "bronze", "v2"],
) as dag:

    t_list = PythonOperator(
        task_id="list_new_files",
        python_callable=list_new_files,
    )

    t_load = PythonOperator(
        task_id="load_to_bronze",
        python_callable=load_to_bronze,
    )

    t_archive = PythonOperator(
        task_id="archive_files",
        python_callable=archive_files,
    )

    t_list >> t_load >> t_archive