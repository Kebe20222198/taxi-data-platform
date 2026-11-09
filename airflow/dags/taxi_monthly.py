"""
NYC Taxi Monthly Batch Pipeline — Airflow 3 Orchestration DAG
=============================================================

Pipeline: Ingestion → Bronze → Silver → Data Quality → Gold → dbt → Observability
Architecture: Medallion Architecture (MinIO / S3A + Spark 3.5 + Delta Lake 3.1)
Target Machine: 16 GB RAM / 4 cores (optimized memory budget with sequential backfills)

Key Characteristics:
- DAG ID: taxi_monthly
- Schedule: 0 0 5 * * (5th day of every month at 00:00 UTC)
- Catchup: False (backfills handled via explicit Airflow 3 CLI commands)
- max_active_runs: 1 (strictly sequential execution to avoid RAM saturation)
- Dynamic execution date: {{ logical_date.strftime('%Y-%m') }} passed to each task
- Idempotence: Full partition overwrites in Bronze, Silver (replaceWhere), and Gold
- Quality Gate: Silver partition non-empty check + quarantine rejection rate threshold (< 20%)
- Observability: on_failure_callback blocking downstream runs & tracking in PostgreSQL pipeline_runs
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

from airflow.exceptions import AirflowException

# Structured Logger
log = logging.getLogger("airflow.taxi_monthly")

# ==============================================================================
# Configuration & Constants
# ==============================================================================

DAG_ID = "taxi_monthly"
SCHEDULE = "0 0 5 * *"
START_DATE = datetime(2025, 1, 1)

# PostgreSQL DWH & Observability Connection (Separate from Airflow Metadata DB)
DWH_HOST = os.getenv("DWH_POSTGRES_HOST", "postgres")
DWH_PORT = int(os.getenv("DWH_POSTGRES_PORT", "5432"))
DWH_DB = os.getenv("DWH_POSTGRES_DB", "airflow")
DWH_USER = os.getenv("DWH_POSTGRES_USER", "airflow")
DWH_PASSWORD = os.getenv("DWH_POSTGRES_PASSWORD", "airflow")

# MinIO / S3 Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", os.getenv("MINIO_ROOT_USER", "minioadmin"))
MINIO_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))

# Critical Data Quality Thresholds
MAX_ALLOWED_REJECTION_RATE: float = 0.20  # 20.0% max rejection threshold
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL")  # Optional Slack/Teams webhook

# ==============================================================================
# Observability Helper: PostgreSQL pipeline_runs Recorder
# ==============================================================================

def get_dwh_connection():
    """Create a psycopg2 connection to the DWH PostgreSQL database."""
    try:
        import psycopg2
        return psycopg2.connect(
            host=DWH_HOST,
            port=DWH_PORT,
            dbname=DWH_DB,
            user=DWH_USER,
            password=DWH_PASSWORD,
            connect_timeout=10,
        )
    except Exception as exc:
        log.warning(f"Could not connect to PostgreSQL DWH ({DWH_HOST}:{DWH_PORT}): {exc}")
        return None


def record_pipeline_run_metric(
    run_id: str,
    dag_id: str,
    task_id: str | None,
    logical_date: datetime,
    execution_month: str,
    status: str,
    duration_seconds: float = 0.0,
    records_processed: int = 0,
    records_rejected: int = 0,
    rejection_rate: float = 0.0,
    error_message: str | None = None,
) -> None:
    """Record or update pipeline execution telemetry in the pipeline_runs table."""
    conn = get_dwh_connection()
    if conn is None:
        log.info(
            f"Telemetry [pipeline_runs fallback]: run_id={run_id}, month={execution_month}, "
            f"status={status}, processed={records_processed}, rejected={records_rejected}, "
            f"error={error_message}"
        )
        return

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS pipeline_runs (
        run_id VARCHAR(255) PRIMARY KEY,
        dag_id VARCHAR(100) NOT NULL,
        task_id VARCHAR(100),
        logical_date TIMESTAMP WITH TIME ZONE NOT NULL,
        execution_month VARCHAR(7) NOT NULL,
        status VARCHAR(50) NOT NULL,
        duration_seconds NUMERIC(10, 2) DEFAULT 0.0,
        records_processed BIGINT DEFAULT 0,
        records_rejected BIGINT DEFAULT 0,
        rejection_rate NUMERIC(6, 4) DEFAULT 0.0,
        error_message TEXT,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );
    CREATE INDEX IF NOT EXISTS idx_pipeline_runs_month ON pipeline_runs(execution_month);
    CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON pipeline_runs(status);
    """

    upsert_sql = """
    INSERT INTO pipeline_runs (
        run_id, dag_id, task_id, logical_date, execution_month,
        status, duration_seconds, records_processed, records_rejected,
        rejection_rate, error_message, updated_at
    ) VALUES (
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, CURRENT_TIMESTAMP
    )
    ON CONFLICT (run_id) DO UPDATE SET
        task_id = EXCLUDED.task_id,
        status = EXCLUDED.status,
        duration_seconds = EXCLUDED.duration_seconds,
        records_processed = GREATEST(pipeline_runs.records_processed, EXCLUDED.records_processed),
        records_rejected = GREATEST(pipeline_runs.records_rejected, EXCLUDED.records_rejected),
        rejection_rate = EXCLUDED.rejection_rate,
        error_message = EXCLUDED.error_message,
        updated_at = CURRENT_TIMESTAMP;
    """

    try:
        with conn.cursor() as cur:
            cur.execute(create_table_sql)
            cur.execute(
                upsert_sql,
                (
                    run_id,
                    dag_id,
                    task_id,
                    logical_date,
                    execution_month,
                    status,
                    duration_seconds,
                    records_processed,
                    records_rejected,
                    rejection_rate,
                    error_message,
                ),
            )
        conn.commit()
        log.info(f"✅ Telemetry recorded in pipeline_runs for run_id={run_id} (status={status})")
    except Exception as exc:
        conn.rollback()
        log.error(f"Failed to record pipeline_runs telemetry: {exc}")
    finally:
        conn.close()


# ==============================================================================
# Failure Callback — Halts pipeline, logs structured error, updates pipeline_runs
# ==============================================================================

def pipeline_failure_callback(context: dict[str, Any]) -> None:
    """Triggered upon task failure.
    
    1. Blocks downstream execution (via strict trigger rules).
    2. Logs structured JSON error with comprehensive context.
    3. Persists failure state in DWH pipeline_runs observability table.
    4. Optionally sends external alert payload (Slack/Webhook).
    """
    ti = context.get("task_instance")
    task_id = ti.task_id if ti else "unknown"
    dag_id = ti.dag_id if ti else DAG_ID
    run_id = context.get("run_id", f"{dag_id}_unknown")
    logical_date = context.get("logical_date") or datetime.now(timezone.utc)
    month_str = logical_date.strftime("%Y-%m")
    duration = getattr(ti, "duration", 0.0) or 0.0
    exception = context.get("exception")
    error_msg = str(exception) if exception else "Task failed without explicit exception"

    alert_payload = {
        "event": "PIPELINE_EXECUTION_FAILURE",
        "dag_id": dag_id,
        "task_id": task_id,
        "run_id": run_id,
        "logical_date": logical_date.isoformat(),
        "month": month_str,
        "duration_seconds": round(duration, 2),
        "error_message": error_msg,
        "log_url": getattr(ti, "log_url", ""),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "severity": "CRITICAL",
    }

    # Structured JSON log
    log.error(f"🚨 PIPELINE FAILURE DETECTED: {json.dumps(alert_payload, indent=2)}")

    # Record in DWH observability table
    record_pipeline_run_metric(
        run_id=run_id,
        dag_id=dag_id,
        task_id=task_id,
        logical_date=logical_date,
        execution_month=month_str,
        status="FAILED",
        duration_seconds=duration,
        error_message=error_msg[:1000],
    )

    # Optional Webhook Notification
    if ALERT_WEBHOOK_URL:
        try:
            import requests
            requests.post(
                ALERT_WEBHOOK_URL,
                json=alert_payload,
                timeout=5,
            )
            log.info("Webhook alert dispatched successfully.")
        except Exception as exc:
            log.warning(f"Could not dispatch webhook alert: {exc}")


# ==============================================================================
# Quality Checks: Silver Partition Non-Empty + Quarantine Rejection Threshold
# ==============================================================================

def check_silver_quality(month: str, **context: Any) -> dict[str, Any]:
    """Validate data quality post-Silver transformation.
    
    Checks:
    1. Silver partition exists and contains at least 1 valid record.
    2. Quarantine rejection rate does not exceed critical threshold (20%).
    
    If any check fails, an AirflowException is raised, immediately stopping
    downstream stages (silver_to_gold, dbt_run, etc.).
    """
    ti = context["task_instance"]
    run_id = context.get("run_id", f"{DAG_ID}_{month}")
    logical_date = context.get("logical_date") or datetime.now(timezone.utc)

    log.info(f"🔍 Running Silver Data Quality Checks for month: {month}")

    # Read transformation metrics from bronze_to_silver XCom if available
    raw_xcom = ti.xcom_pull(task_ids="bronze_to_silver", key="return_value")
    b2s_metrics: dict[str, Any] = {}
    if isinstance(raw_xcom, dict):
        b2s_metrics = raw_xcom
    elif isinstance(raw_xcom, str):
        try:
            b2s_metrics = json.loads(raw_xcom)
        except Exception:
            b2s_metrics = {}

    silver_count = b2s_metrics.get("silver_count")
    rejected_count = b2s_metrics.get("rejected_count", 0)

    # If not in XCom (e.g. bash execution), verify directly via MinIO S3 API
    if silver_count is None:
        try:
            from minio import Minio
            clean_endpoint = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
            client = Minio(
                clean_endpoint,
                access_key=MINIO_ACCESS_KEY,
                secret_key=MINIO_SECRET_KEY,
                secure=MINIO_ENDPOINT.startswith("https"),
            )
            year, m = month.split("-")
            month_int = str(int(m))
            # Match both month=1 and month=01
            silver_prefixes = [f"yellow/year={year}/month={month_int}/", f"yellow/year={year}/month={m}/"]
            silver_objects = []
            for pfx in silver_prefixes:
                objs = list(client.list_objects("silver", prefix=pfx, recursive=True))
                if objs:
                    silver_objects = objs
                    break

            quarantine_prefixes = [f"yellow/year={year}/month={m}/", f"yellow/year={year}/month={month_int}/"]
            quarantine_objects = []
            for pfx in quarantine_prefixes:
                objs = list(client.list_objects("quarantine", prefix=pfx, recursive=True))
                if objs:
                    quarantine_objects = objs
                    break

            total_silver_size = sum(obj.size for obj in silver_objects if obj.size)
            total_quarantine_size = sum(obj.size for obj in quarantine_objects if obj.size)

            log.info(
                f"MinIO S3 Verification: Silver partition has {len(silver_objects)} objects ({total_silver_size} bytes); "
                f"Quarantine has {len(quarantine_objects)} objects ({total_quarantine_size} bytes)"
            )

            # Valid partition found
            if total_silver_size > 0:
                # Estimate row counts from data volume (~187 bytes/row in Delta Silver, ~27 bytes/row in Quarantine Parquet)
                silver_count = max(int(total_silver_size / 187), len(silver_objects))
                rejected_count = int(total_quarantine_size / 27)
                total_bytes = total_silver_size + total_quarantine_size
                rejection_rate = (total_quarantine_size / total_bytes) if total_bytes > 0 else 0.0
            else:
                silver_count = 0
                rejected_count = 0
                rejection_rate = 0.0
        except Exception as exc:
            log.error(f"Could not connect to MinIO for direct S3 validation: {exc}")
            raise AirflowException(f"Quality check failed: unable to verify Silver partition via MinIO: {exc}")

    total_records = silver_count + rejected_count
    if "rejection_rate" not in locals():
        rejection_rate = (rejected_count / total_records) if total_records > 0 else 0.0

    qc_summary = {
        "month": month,
        "silver_records": silver_count,
        "quarantine_rejected_records": rejected_count,
        "total_records": total_records,
        "rejection_rate": round(rejection_rate, 4),
        "rejection_rate_percent": f"{rejection_rate * 100:.2f}%",
        "max_allowed_threshold_percent": f"{MAX_ALLOWED_REJECTION_RATE * 100:.2f}%",
        "passed": False,
    }

    log.info(f"📊 Quality Check Metrics: {json.dumps(qc_summary, indent=2)}")

    # ── Check 1: Non-Empty Silver Partition ──────────────────────────────
    if silver_count <= 0:
        error_msg = (
            f"❌ CRITICAL QUALITY CHECK FAILED: Silver partition {month} is EMPTY (0 rows). "
            f"Raw Bronze data may be missing, corrupted, or completely filtered out."
        )
        log.error(error_msg)
        raise AirflowException(error_msg)

    # ── Check 2: Quarantine Rejection Rate Threshold ─────────────────────
    if rejection_rate > MAX_ALLOWED_REJECTION_RATE:
        error_msg = (
            f"❌ CRITICAL QUALITY CHECK FAILED: Quarantine rejection rate for {month} is "
            f"{rejection_rate * 100:.2f}% ({rejected_count:,} rejected / {total_records:,} total), "
            f"exceeding the critical safety threshold of {MAX_ALLOWED_REJECTION_RATE * 100:.2f}%! "
            f"Potential schema drift, parser corruption, or upstream sensor failure."
        )
        log.error(error_msg)
        raise AirflowException(error_msg)

    qc_summary["passed"] = True
    log.info(f"✅ Quality Check PASSED: Silver partition {month} is valid and meets quality SLAs.")

    # Record passing quality metrics in DWH
    record_pipeline_run_metric(
        run_id=run_id,
        dag_id=DAG_ID,
        task_id="quality_checks_silver",
        logical_date=logical_date,
        execution_month=month,
        status="RUNNING",
        records_processed=silver_count,
        records_rejected=rejected_count,
        rejection_rate=rejection_rate,
    )

    ti.xcom_push(key="qc_summary", value=qc_summary)
    return qc_summary


# ==============================================================================
# Final Success Notification & Telemetry Completion
# ==============================================================================

def pipeline_success_notification(month: str, **context: Any) -> None:
    """Record final pipeline success in pipeline_runs table and log summary."""
    ti = context["task_instance"]
    run_id = context.get("run_id", f"{DAG_ID}_{month}")
    logical_date = context.get("logical_date") or datetime.now(timezone.utc)
    dag_run = context.get("dag_run")

    duration = 0.0
    if dag_run and dag_run.start_date:
        now = datetime.now(timezone.utc)
        duration = (now - dag_run.start_date).total_seconds()

    qc_summary = ti.xcom_pull(task_ids="quality_checks_silver", key="qc_summary") or {}
    processed = qc_summary.get("silver_records", 0)
    rejected = qc_summary.get("quarantine_rejected_records", 0)
    rejection_rate = qc_summary.get("rejection_rate", 0.0)

    summary = {
        "event": "PIPELINE_EXECUTION_SUCCESS",
        "dag_id": DAG_ID,
        "run_id": run_id,
        "month": month,
        "status": "SUCCESS",
        "duration_seconds": round(duration, 2),
        "records_processed": processed,
        "records_quarantined": rejected,
        "rejection_rate": f"{rejection_rate * 100:.2f}%",
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }

    log.info(f"🎉 PIPELINE COMPLETED SUCCESSFULLY:\n{json.dumps(summary, indent=2)}")

    record_pipeline_run_metric(
        run_id=run_id,
        dag_id=DAG_ID,
        task_id="notify",
        logical_date=logical_date,
        execution_month=month,
        status="SUCCESS",
        duration_seconds=duration,
        records_processed=processed,
        records_rejected=rejected,
        rejection_rate=rejection_rate,
        error_message=None,
    )


# ==============================================================================
# DAG Definition
# ==============================================================================

default_args = {
    "owner": "dataops",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": pipeline_failure_callback,
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id=DAG_ID,
    description="Monthly idempotent orchestration: NYC Yellow Taxi Medallion Pipeline",
    schedule=SCHEDULE,
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["nyc-taxi", "phase-3", "medallion", "idempotent", "airflow-3"],
    doc_md=__doc__,
) as dag:

    # ── Task 1: Ingestion (Bronze Layer) ─────────────────────────────────────
    # Downloads raw monthly Parquet + references to MinIO Bronze layer with SHA-256 idempotence.
    ingest = BashOperator(
        task_id="ingest",
        bash_command=(
            "python3 /opt/airflow/ingestion/ingest.py "
            "--month {{ logical_date.strftime('%Y-%m') }} "
            "--endpoint http://minio:9000"
        ),
        env={
            "MINIO_ENDPOINT_HOST": "http://minio:9000",
            "MINIO_ROOT_USER": MINIO_ACCESS_KEY,
            "MINIO_ROOT_PASSWORD": MINIO_SECRET_KEY,
        },
    )

    # ── Task 2: Bronze → Silver (Spark Transformation + Quarantine) ───────────
    # Casts types, applies 5 quality rules, pushes invalid rows to quarantine,
    # deduplicates with SHA-256, and writes Delta partition (replaceWhere).
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            "docker exec taxi-spark-master /opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "--jars /opt/spark/jars_extra/delta-spark_2.12-3.1.0.jar,/opt/spark/jars_extra/delta-storage-3.1.0.jar,/opt/spark/jars_extra/hadoop-aws-3.3.4.jar,/opt/spark/jars_extra/aws-java-sdk-bundle-1.12.262.jar "
            "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
            "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
            "/opt/spark_jobs/bronze_to_silver.py "
            "--month {{ logical_date.strftime('%Y-%m') }}"
        ),
    )

    # ── Task 3: Quality Checks (Silver Quality Gate) ─────────────────────────
    # Verifies that written partition is non-empty and quarantine rejection rate <= 20%.
    quality_checks_silver = PythonOperator(
        task_id="quality_checks_silver",
        python_callable=check_silver_quality,
        op_kwargs={"month": "{{ logical_date.strftime('%Y-%m') }}"},
    )

    # ── Task 4: Silver → Gold (Spark Aggregations) ───────────────────────────
    # Joins taxi zone reference table, computes hourly-zone KPIs,
    # and writes Delta Lake Gold layer with replaceWhere idempotence.
    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=(
            "docker exec taxi-spark-master /opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "--jars /opt/spark/jars_extra/delta-spark_2.12-3.1.0.jar,/opt/spark/jars_extra/delta-storage-3.1.0.jar,/opt/spark/jars_extra/hadoop-aws-3.3.4.jar,/opt/spark/jars_extra/aws-java-sdk-bundle-1.12.262.jar "
            "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
            "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
            "/opt/spark_jobs/silver_to_gold.py "
            "--month {{ logical_date.strftime('%Y-%m') }}"
        ),
    )

    # ── Task 5: dbt Run (DWH Dimensional Modeling on DuckDB) ────────────────
    # Transforms staging views and fact/dimension tables in DuckDB scanning Delta Lake.
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "/home/airflow/.local/bin/dbt run --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt "
            "--vars '{\"month\": \"{{ logical_date.strftime(\"%Y-%m\") }}\"}'"
        ),
        env={
            "HOME": "/home/airflow",
            "PATH": "/home/airflow/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            "DBT_TARGET": "duckdb",
            "DUCKDB_PATH": "/opt/airflow/data/taxi_warehouse.duckdb",
            "MINIO_ENDPOINT": "http://minio:9000",
            "AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
            "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY,
            "DWH_POSTGRES_HOST": DWH_HOST,
            "DWH_POSTGRES_PORT": str(DWH_PORT),
            "DWH_POSTGRES_DB": DWH_DB,
            "DWH_POSTGRES_USER": DWH_USER,
            "DWH_POSTGRES_PASSWORD": DWH_PASSWORD,
        },
    )

    # ── Task 6: dbt Test (Schema & Integrity Tests on DuckDB) ───────────────
    # Runs automated tests: not null, unique keys, accepted values, referential integrity.
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "/home/airflow/.local/bin/dbt test --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt"
        ),
        env={
            "HOME": "/home/airflow",
            "PATH": "/home/airflow/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            "DBT_TARGET": "duckdb",
            "DUCKDB_PATH": "/opt/airflow/data/taxi_warehouse.duckdb",
            "MINIO_ENDPOINT": "http://minio:9000",
            "AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
            "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY,
            "DWH_POSTGRES_HOST": DWH_HOST,
            "DWH_POSTGRES_PORT": str(DWH_PORT),
            "DWH_POSTGRES_DB": DWH_DB,
            "DWH_POSTGRES_USER": DWH_USER,
            "DWH_POSTGRES_PASSWORD": DWH_PASSWORD,
        },
    )

    # ── Task 7: Notify (Pipeline Telemetry & Final Observability) ────────────
    # Marks run SUCCESS, updates DWH pipeline_runs, and logs structured metrics.
    notify = PythonOperator(
        task_id="notify",
        python_callable=pipeline_success_notification,
        op_kwargs={"month": "{{ logical_date.strftime('%Y-%m') }}"},
    )

    # ── Strict Linear Dependency Enchainment ──────────────────────────────────
    # ingest -> bronze_to_silver -> quality_checks_silver -> silver_to_gold -> dbt_run -> dbt_test -> notify
    (
        ingest
        >> bronze_to_silver
        >> quality_checks_silver
        >> silver_to_gold
        >> dbt_run
        >> dbt_test
        >> notify
    )
