#!/usr/bin/env python3
"""
bronze_to_silver.py
~~~~~~~~~~~~~~~~~~~
Job Spark : Bronze → Silver pour les données NYC Yellow Taxi.

Pipeline
--------
1. Lecture de la partition bronze ``s3a://bronze/yellow/year=YYYY/month=MM/``
   avec le schéma explicite ``YELLOW_TAXI_RAW_SCHEMA`` (aucune inférence).
2. Renommage snake_case + cast des types.
3. Application des 5 règles qualité → split valid / rejected.
4. Écriture des rejets dans ``s3a://quarantine/yellow/year=YYYY/month=MM/``
   avec la colonne ``reject_reason``.
5. Ajout des colonnes dérivées sur les lignes valides.
6. Dédoublonnage SHA-256 sur les colonnes identifiantes du trajet.
7. Écriture Delta Lake dans ``s3a://silver/yellow/`` avec ``replaceWhere``
   (idempotence totale : on réécrit uniquement la partition du mois traité).

Usage
-----
    spark-submit \\
        --master spark://spark-master:7077 \\
        --packages io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4 \\
        bronze_to_silver.py --month 2025-01
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

# ── Make spark_jobs package importable when running via spark-submit ──────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import functions as F

from spark_jobs.common.constants import (
    BRONZE_YELLOW_PATH,
    QUARANTINE_YELLOW_PATH,
    SILVER_YELLOW_PATH,
)
from spark_jobs.common.schemas import YELLOW_TAXI_RAW_SCHEMA
from spark_jobs.common.spark_factory import create_spark_session
from spark_jobs.common.transformations import (
    add_derived_columns,
    add_partition_columns,
    apply_quality_rules,
    cast_types,
    deduplicate,
    rename_columns,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("bronze_to_silver")


# ==============================================================================
# Main job logic
# ==============================================================================

def run(year: int, month: int) -> dict:
    """Execute the Bronze → Silver transformation for one month.

    Returns
    -------
    dict
        Run metrics (counts, duration, status).
    """
    t0 = time.monotonic()
    month_str = f"{year:04d}-{month:02d}"
    log.info(f"▶ Starting Bronze → Silver for {month_str}")

    # ── 1. SparkSession ──────────────────────────────────────────────────
    spark = create_spark_session(app_name=f"bronze-to-silver-{month_str}")

    # ── 2. Read bronze partition with explicit schema ────────────────────
    bronze_path = f"{BRONZE_YELLOW_PATH}/year={year:04d}/month={month:02d}"
    log.info(f"  Reading from {bronze_path}")
    df_raw = (
        spark.read
        .schema(YELLOW_TAXI_RAW_SCHEMA)
        .parquet(bronze_path)
    )
    raw_count = df_raw.count()
    log.info(f"  Raw row count: {raw_count:,}")

    # ── 3. Rename + Cast ─────────────────────────────────────────────────
    df_clean = cast_types(rename_columns(df_raw))

    # ── 4. Quality rules → valid / rejected ──────────────────────────────
    df_valid, df_rejected = apply_quality_rules(df_clean, year, month)

    rejected_count = df_rejected.count()
    log.info(f"  Rejected rows: {rejected_count:,}")

    # ── 5. Write rejected to quarantine ──────────────────────────────────
    if rejected_count > 0:
        quarantine_path = (
            f"{QUARANTINE_YELLOW_PATH}/year={year:04d}/month={month:02d}"
        )
        log.info(f"  Writing rejects to {quarantine_path}")
        df_rejected_partitioned = add_partition_columns(df_rejected, year, month)
        (
            df_rejected_partitioned
            .write
            .mode("overwrite")
            .parquet(quarantine_path)
        )

    # ── 6. Enrich valid rows ─────────────────────────────────────────────
    df_enriched = add_derived_columns(df_valid)

    # ── 7. Deduplicate ───────────────────────────────────────────────────
    df_deduped = deduplicate(df_enriched)
    deduped_count = df_deduped.count()
    duplicates_removed = df_valid.count() - deduped_count
    log.info(f"  Duplicates removed: {duplicates_removed:,}")

    # ── 8. Add partition columns & write Silver (Delta) ──────────────────
    df_final = add_partition_columns(df_deduped, year, month)

    log.info(f"  Writing {deduped_count:,} rows to Silver (Delta)")
    (
        df_final
        .write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"year = {year} AND month = {month}")
        .partitionBy("year", "month")
        .save(SILVER_YELLOW_PATH)
    )

    duration_s = round(time.monotonic() - t0, 2)

    metrics = {
        "month": month_str,
        "raw_count": raw_count,
        "rejected_count": rejected_count,
        "duplicates_removed": duplicates_removed,
        "silver_count": deduped_count,
        "duration_s": duration_s,
        "status": "success",
    }
    log.info(f"✅ Bronze → Silver completed in {duration_s}s: {metrics}")

    spark.stop()
    return metrics


# ==============================================================================
# CLI
# ==============================================================================

def parse_month(value: str) -> tuple[int, int]:
    """Parse YYYY-MM → (year, month)."""
    try:
        dt = datetime.strptime(value, "%Y-%m")
        return dt.year, dt.month
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Format invalide '{value}'. Attendu : YYYY-MM (ex: 2024-01)."
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Spark job: Bronze → Silver (Yellow Taxi)",
    )
    parser.add_argument(
        "--month",
        type=str,
        required=True,
        help="Mois à traiter au format YYYY-MM.",
    )
    args = parser.parse_args()
    year, month = parse_month(args.month)

    metrics = run(year, month)
    if metrics["status"] != "success":
        sys.exit(1)


if __name__ == "__main__":
    main()
