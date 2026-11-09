#!/usr/bin/env python3
"""
NYC Taxi Data Platform — Performance Benchmarking Pipeline
===========================================================

Phase 5: Experimental Performance Tuning & Optimization Study
Dataset: 12 months of Yellow Taxi trip records (~35-40M rows)
Target Machine: 16 GB RAM / 4 vCPU local environment

Optimization Levels (Progressive or Isolated):
----------------------------------------------
  Level 0 : Naive Baseline
            - inferSchema=True
            - Read without partition pruning (scans full bronze directory)
            - Standard SortMergeJoin on taxi zones (autoBroadcastJoinThreshold = -1)
            - spark.sql.shuffle.partitions = 200 (Spark default)
            - No skew handling (heavy stragglers on JFK/Midtown zones)
            - No cache (lineage re-executed 3x for quarantine, silver, and gold)
            - No file compaction (produces hundreds of small files)
            - AQE disabled (spark.sql.adaptive.enabled = false)

  Level 1 : + Explicit Schema (YELLOW_TAXI_RAW_SCHEMA, eliminates preliminary file scans)
  Level 2 : + Partition Pruning (targeted read on year=YYYY/month=MM)
  Level 3 : + BroadcastHashJoin (F.broadcast on dim_zone lookup of 265 rows)
  Level 4 : + Tuned Shuffle Partitions (spark.sql.shuffle.partitions = 8 for 4 cores)
  Level 5 : + Skew Handling (Key Salting on high-volume pickup zones)
  Level 6 : + DataFrame Persistence (persist MEMORY_AND_DISK before branching)
  Level 7 : + Output Compaction (coalesce/repartition before storage write)
  Level 8 : + Full Adaptive Query Execution (AQE + dynamic coalesce + skew join)

Usage:
------
  python benchmark_pipeline.py --opt-level 0 --year 2024
  python benchmark_pipeline.py --opt-level 8 --year 2024
  python benchmark_pipeline.py --opt-only 3 --year 2024  # Isolate only Broadcast Join
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from spark_jobs.common.constants import (
    BRONZE_REFERENCE_PATH,
    BRONZE_YELLOW_PATH,
    GOLD_METRICS_PATH,
    QUARANTINE_YELLOW_PATH,
    SILVER_YELLOW_PATH,
    VALID_ZONE_IDS,
)
from spark_jobs.common.schemas import YELLOW_TAXI_RAW_SCHEMA
from spark_jobs.common.spark_factory import create_spark_session
from spark_jobs.common.transformations import (
    add_derived_columns,
    add_partition_columns,
    cast_types,
    deduplicate,
    rename_columns,
)
from spark_jobs.silver_to_gold import TAXI_ZONE_SCHEMA

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("spark_benchmark")

# High-frequency skewed zones in NYC (JFK Airport, Times Square, Midtown)
SKEWED_ZONE_IDS: set[int] = {132, 138, 161, 230, 237, 142}
SALT_FACTOR: int = 4  # Salting factor to divide skewed partition volume into 4 sub-keys


# ==============================================================================
# Benchmarking Configuration Resolver
# ==============================================================================

class BenchmarkConfig:
    """Manages individual optimization toggles based on opt-level or isolated flag."""

    def __init__(self, opt_level: int = 0, opt_only: int | None = None):
        self.opt_level = opt_level
        self.opt_only = opt_only

        if opt_only is not None:
            # Isolated mode: baseline with ONLY this specific optimization active
            self.explicit_schema = (opt_only == 1)
            self.partition_pruning = (opt_only == 2)
            self.broadcast_join = (opt_only == 3)
            self.tuned_shuffle_partitions = (opt_only == 4)
            self.skew_salting = (opt_only == 5)
            self.dataframe_caching = (opt_only == 6)
            self.file_compaction = (opt_only == 7)
            self.aqe_enabled = (opt_only == 8)
        else:
            # Progressive cumulative mode: Level N activates all optimizations <= N
            self.explicit_schema = (opt_level >= 1)
            self.partition_pruning = (opt_level >= 2)
            self.broadcast_join = (opt_level >= 3)
            self.tuned_shuffle_partitions = (opt_level >= 4)
            self.skew_salting = (opt_level >= 5)
            self.dataframe_caching = (opt_level >= 6)
            self.file_compaction = (opt_level >= 7)
            self.aqe_enabled = (opt_level >= 8)

    def summary(self) -> dict[str, bool]:
        return {
            "opt_level": self.opt_level,
            "opt_only": self.opt_only,
            "explicit_schema (Opt 1)": self.explicit_schema,
            "partition_pruning (Opt 2)": self.partition_pruning,
            "broadcast_join (Opt 3)": self.broadcast_join,
            "tuned_shuffle_partitions (Opt 4)": self.tuned_shuffle_partitions,
            "skew_salting (Opt 5)": self.skew_salting,
            "dataframe_caching (Opt 6)": self.dataframe_caching,
            "file_compaction (Opt 7)": self.file_compaction,
            "aqe_enabled (Opt 8)": self.aqe_enabled,
        }


# ==============================================================================
# Spark Session Construction for Benchmarking
# ==============================================================================

def build_benchmark_spark_session(cfg: BenchmarkConfig, app_name: str) -> SparkSession:
    """Build a SparkSession tuned strictly to the active benchmark configuration."""
    builder = SparkSession.builder.appName(app_name)

    # Master configuration
    master = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
    builder = builder.master(master)

    # S3A MinIO credentials & Delta Lake extensions
    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("AWS_ACCESS_KEY_ID", os.getenv("MINIO_ROOT_USER", "minioadmin"))
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))

    builder = (
        builder
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    )

    # ── Optimisation 4: Shuffle Partitions ───────────────────────────────
    if cfg.tuned_shuffle_partitions:
        # 4 cores on machine -> 8 partitions (2 tasks per core optimal throughput)
        shuffle_parts = "8"
    else:
        # Default unoptimized Spark behavior
        shuffle_parts = "200"
    builder = builder.config("spark.sql.shuffle.partitions", shuffle_parts)

    # ── Optimisation 3: Broadcast Join Threshold ─────────────────────────
    if cfg.broadcast_join:
        # Default 10MB or higher allows broadcast
        builder = builder.config("spark.sql.autoBroadcastJoinThreshold", "10485760")
    else:
        # -1 completely disables automatic broadcast join, forcing SortMergeJoin
        builder = builder.config("spark.sql.autoBroadcastJoinThreshold", "-1")

    # ── Optimisation 8: Adaptive Query Execution (AQE) ───────────────────
    if cfg.aqe_enabled:
        builder = (
            builder
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "67108864")  # 64 MB
        )
    else:
        builder = (
            builder
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
            .config("spark.sql.adaptive.skewJoin.enabled", "false")
        )

    return builder.getOrCreate()


# ==============================================================================
# Pipeline Stages Implementation with Progressive Optimizations
# ==============================================================================

def load_bronze_data(spark: SparkSession, cfg: BenchmarkConfig, year: int, months: list[int]) -> DataFrame:
    """Stage 1: Read raw bronze data (Opt 1: Explicit Schema, Opt 2: Partition Pruning)."""
    t0 = time.monotonic()

    # ── Optimisation 2: Partition Pruning ─────────────────────────────────
    if cfg.partition_pruning:
        # Target specific year/month partition folders
        paths = [
            f"{BRONZE_YELLOW_PATH}/year={year:04d}/month={m:02d}"
            for m in months
        ]
        log.info(f"  [Opt 2 ACTIVE] Partition pruning enabled: scanning {len(paths)} target partition folders")
    else:
        # Naive: reads entire bucket hierarchy with recursive wildcard, filtering downstream
        paths = [f"{BRONZE_YELLOW_PATH}/*/*/*.parquet"]
        log.info("  [Opt 2 INACTIVE] Full directory scan without pruning: reading entire bronze hierarchy")

    # ── Optimisation 1: Explicit Schema ──────────────────────────────────
    reader = spark.read
    if cfg.explicit_schema:
        log.info("  [Opt 1 ACTIVE] Explicit StructType schema provided (zero schema inference I/O)")
        df_raw = reader.schema(YELLOW_TAXI_RAW_SCHEMA).parquet(*paths)
    else:
        log.info("  [Opt 1 INACTIVE] Schema inference enabled (triggering file metadata & footer read)")
        # Reading without schema triggers Parquet footer inspection across all files
        df_raw = reader.option("inferSchema", "true").parquet(*paths)

    # Downstream filter required if partition pruning was not used
    if not cfg.partition_pruning:
        # Simulates filtering inside Catalyst without storage-level partition pushdown
        df_raw = df_raw.filter(
            (F.year("tpep_pickup_datetime") == year) &
            (F.month("tpep_pickup_datetime").isin(months))
        )

    log.info(f"  Stage 1 (Bronze Load) prepared in {time.monotonic() - t0:.2f}s")
    return df_raw


def execute_silver_cleaning(df_raw: DataFrame, cfg: BenchmarkConfig, year: int) -> tuple[DataFrame, DataFrame]:
    """Stage 2: Clean, validate quality rules, and split quarantine."""
    t0 = time.monotonic()

    # Rename & Cast
    df_clean = cast_types(rename_columns(df_raw))

    # Duration helper
    df_clean = df_clean.withColumn(
        "_duration_s",
        F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")
    )

    # Quality Rules split
    valid_cond = (
        F.col("_duration_s").isNotNull() &
        (F.col("_duration_s") > 0) &
        (F.col("_duration_s") <= 21600) &
        F.col("trip_distance").isNotNull() &
        (F.col("trip_distance") > 0) &
        (F.col("trip_distance") <= 100.0) &
        F.col("total_amount").isNotNull() &
        (F.col("total_amount") > 0) &
        F.col("pu_location_id").isNotNull() &
        F.col("do_location_id").isNotNull() &
        F.col("pu_location_id").isin(list(VALID_ZONE_IDS)) &
        F.col("do_location_id").isin(list(VALID_ZONE_IDS))
    )

    df_valid = df_clean.filter(valid_cond).drop("_duration_s")
    df_rejected = df_clean.filter(~valid_cond).withColumn("reject_reason", F.lit("quality_rule_violation")).drop("_duration_s")

    # Enrich valid data
    df_enriched = add_derived_columns(df_valid)
    df_deduped = deduplicate(df_enriched)
    df_final_silver = df_deduped.withColumn("year", F.year("tpep_pickup_datetime")).withColumn("month", F.month("tpep_pickup_datetime"))

    # ── Optimisation 6: DataFrame Caching ─────────────────────────────────
    if cfg.dataframe_caching:
        log.info("  [Opt 6 ACTIVE] Caching cleaned Silver DataFrame in MEMORY_AND_DISK")
        df_final_silver = df_final_silver.persist(StorageLevel.MEMORY_AND_DISK)
    else:
        log.info("  [Opt 6 INACTIVE] No caching: lineage will be re-evaluated for each terminal action")

    log.info(f"  Stage 2 (Silver Transform) prepared in {time.monotonic() - t0:.2f}s")
    return df_final_silver, df_rejected


def execute_gold_aggregation(
    df_silver: DataFrame,
    df_zones: DataFrame,
    cfg: BenchmarkConfig,
) -> DataFrame:
    """Stage 3: Join zones and compute hourly KPIs (Opt 3: Broadcast, Opt 5: Skew Salting)."""
    t0 = time.monotonic()

    # ── Optimisation 5: Skew Handling / Key Salting ───────────────────────
    if cfg.skew_salting:
        log.info(f"  [Opt 5 ACTIVE] Key salting enabled on skewed zones: factor={SALT_FACTOR}")
        # Add random salt (0 to SALT_FACTOR-1) for skewed pickup location keys
        df_silver_salted = df_silver.withColumn(
            "salt",
            F.when(
                F.col("pu_location_id").isin(list(SKEWED_ZONE_IDS)),
                F.floor(F.rand() * SALT_FACTOR)
            ).otherwise(F.lit(0))
        ).withColumn(
            "salted_pu_key",
            F.concat_ws("_", F.col("pu_location_id"), F.col("salt"))
        )

        # Replicate small zones table across all salt values (0..SALT_FACTOR-1)
        salts_df = df_silver.sparkSession.range(SALT_FACTOR).withColumnRenamed("id", "salt")
        df_zones_salted = (
            df_zones
            .crossJoin(salts_df)
            .withColumn("salted_pu_key", F.concat_ws("_", F.col("LocationID"), F.col("salt")))
            .drop("salt")
        )

        # ── Optimisation 3: Broadcast Join with Salting ───────────────────
        if cfg.broadcast_join:
            log.info("  [Opt 3 ACTIVE] BroadcastHashJoin with salted keys")
            joined = df_silver_salted.join(F.broadcast(df_zones_salted), on="salted_pu_key", how="left")
        else:
            log.info("  [Opt 3 INACTIVE] SortMergeJoin with salted keys")
            joined = df_silver_salted.join(df_zones_salted, on="salted_pu_key", how="left")

        joined = joined.drop("salted_pu_key", "salt")
    else:
        log.info("  [Opt 5 INACTIVE] Standard join without key salting")
        # ── Optimisation 3: Broadcast Join without Salting ────────────────
        if cfg.broadcast_join:
            log.info("  [Opt 3 ACTIVE] BroadcastHashJoin on dim_zone lookup table")
            joined = df_silver.join(
                F.broadcast(df_zones.select(F.col("LocationID").alias("pu_loc_id"), F.col("Borough").alias("pu_borough"))),
                df_silver["pu_location_id"] == F.col("pu_loc_id"),
                how="left"
            ).drop("pu_loc_id")
        else:
            log.info("  [Opt 3 INACTIVE] Forcing SortMergeJoin on taxi zones (Exchange + Sort)")
            joined = df_silver.join(
                df_zones.select(F.col("LocationID").alias("pu_loc_id"), F.col("Borough").alias("pu_borough")),
                df_silver["pu_location_id"] == F.col("pu_loc_id"),
                how="left"
            ).drop("pu_loc_id")

    # Aggregate by zone x date x hour
    df_gold = (
        joined
        .groupBy(
            F.to_date("tpep_pickup_datetime").alias("pickup_date"),
            "pickup_hour",
            "pu_location_id",
            "pu_borough"
        )
        .agg(
            F.count("*").alias("trip_count"),
            F.sum("passenger_count").alias("total_passengers"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("tip_amount"), 2).alias("avg_tip_amount"),
            F.round(F.avg("duration_minutes"), 2).alias("avg_duration_minutes"),
            F.round(F.avg("avg_speed_mph"), 2).alias("avg_speed_mph")
        )
    )

    log.info(f"  Stage 3 (Gold Aggregate) prepared in {time.monotonic() - t0:.2f}s")
    return df_gold


def write_benchmarked_outputs(
    df_silver: DataFrame,
    df_rejected: DataFrame,
    df_gold: DataFrame,
    cfg: BenchmarkConfig,
    dry_run: bool = False,
) -> dict[str, int]:
    """Stage 4: Write datasets (Opt 7: Small Files Compaction)."""
    t0 = time.monotonic()

    # ── Optimisation 7: File Compaction ──────────────────────────────────
    if cfg.file_compaction:
        log.info("  [Opt 7 ACTIVE] Output compaction enabled: coalescing to 4 partitions")
        silver_writer = df_silver.coalesce(4)
        rejected_writer = df_rejected.coalesce(2)
        gold_writer = df_gold.coalesce(2)
    else:
        log.info("  [Opt 7 INACTIVE] Direct uncoalesced write: producing many small files")
        silver_writer = df_silver
        rejected_writer = df_rejected
        gold_writer = df_gold

    if dry_run:
        log.info("  [Dry Run] Triggering count actions without actual S3 write")
        silver_cnt = silver_writer.count()
        rejected_cnt = rejected_writer.count()
        gold_cnt = gold_writer.count()
    else:
        # In benchmark mode, write to a designated benchmark output directory
        target_silver = f"{SILVER_YELLOW_PATH}_bench_opt{cfg.opt_level}"
        target_gold = f"{GOLD_METRICS_PATH}_bench_opt{cfg.opt_level}"

        log.info(f"  Writing Silver Delta to {target_silver}...")
        silver_writer.write.format("delta").mode("overwrite").partitionBy("year", "month").save(target_silver)
        silver_cnt = silver_writer.count()

        log.info(f"  Writing Gold Delta to {target_gold}...")
        gold_writer.write.format("delta").mode("overwrite").save(target_gold)
        gold_cnt = gold_writer.count()
        rejected_cnt = rejected_writer.count()

    log.info(f"  Stage 4 (Storage Write) completed in {time.monotonic() - t0:.2f}s")
    return {
        "silver_count": silver_cnt,
        "rejected_count": rejected_cnt,
        "gold_count": gold_cnt,
    }


# ==============================================================================
# Main Benchmark Execution Function
# ==============================================================================

def run_benchmark(
    opt_level: int = 0,
    opt_only: int | None = None,
    year: int = 2024,
    months: list[int] | None = None,
    dry_run: bool = False,
) -> dict:
    """Execute the complete 12-month pipeline under benchmark profiling."""
    if months is None:
        months = list(range(1, 13))  # Full 12 months

    cfg = BenchmarkConfig(opt_level=opt_level, opt_only=opt_only)
    app_name = f"spark-bench-lvl{opt_level}-only{opt_only or 'none'}"

    log.info("=" * 78)
    log.info(f"🚀 STARTING BENCHMARK: Level {opt_level} (Opt Only: {opt_only}) on Year {year} ({len(months)} months)")
    log.info(f"📋 Configuration Matrix:\n{json.dumps(cfg.summary(), indent=2)}")
    log.info("=" * 78)

    t_start = time.monotonic()

    # 1. Spark Session
    spark = build_benchmark_spark_session(cfg, app_name)

    try:
        # Load reference zones
        zones_path = f"{BRONZE_REFERENCE_PATH}/taxi_zone_lookup.csv"
        df_zones = spark.read.option("header", "true").schema(TAXI_ZONE_SCHEMA).csv(zones_path)

        # Pipeline stages
        df_raw = load_bronze_data(spark, cfg, year, months)
        df_silver, df_rejected = execute_silver_cleaning(df_raw, cfg, year)
        df_gold = execute_gold_aggregation(df_silver, df_zones, cfg)

        counts = write_benchmarked_outputs(df_silver, df_rejected, df_gold, cfg, dry_run=dry_run)

        # Unpersist if cached
        if cfg.dataframe_caching:
            df_silver.unpersist()

        total_duration = round(time.monotonic() - t_start, 2)

        # Collect Spark metrics via status tracker
        tracker = spark.sparkContext.statusTracker()
        stage_ids = tracker.getActiveStageIds()

        telemetry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "opt_level": opt_level,
            "opt_only": opt_only,
            "year": year,
            "months_count": len(months),
            "total_duration_seconds": total_duration,
            "counts": counts,
            "config": cfg.summary(),
            "status": "SUCCESS",
        }

        log.info("=" * 78)
        log.info(f"🏁 BENCHMARK COMPLETED: Level {opt_level} finished in {total_duration}s ({total_duration/60:.2f} min)")
        log.info("=" * 78)

        return telemetry

    except Exception as exc:
        total_duration = round(time.monotonic() - t_start, 2)
        log.error(f"❌ BENCHMARK FAILED for Level {opt_level} after {total_duration}s: {exc}", exc_info=True)
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "opt_level": opt_level,
            "opt_only": opt_only,
            "total_duration_seconds": total_duration,
            "status": "FAILED",
            "error": str(exc),
        }
    finally:
        spark.stop()


# ==============================================================================
# CLI Entry Point
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Apache Spark Benchmarking Pipeline (Levels 0 through 8)",
    )
    parser.add_argument(
        "--opt-level",
        type=int,
        default=0,
        choices=list(range(9)),
        help="Optimization level to evaluate (0: Baseline Naive, 8: Fully Optimized)",
    )
    parser.add_argument(
        "--opt-only",
        type=int,
        default=None,
        choices=list(range(1, 9)),
        help="Test ONLY this single optimization against baseline Level 0 (1 to 8)",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2024,
        help="Year to process (default: 2024)",
    )
    parser.add_argument(
        "--months",
        type=str,
        default=None,
        help="Comma-separated list of months (e.g. 1,2,3 or 1-12). Default: full 12 months",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Execute action queries without writing to MinIO",
    )
    parser.add_argument(
        "--output-json",
        type=str,
        default=None,
        help="Path to save execution telemetry JSON",
    )

    args = parser.parse_args()

    months = None
    if args.months:
        months = [int(m.strip()) for m in args.months.split(",")]

    results = run_benchmark(
        opt_level=args.opt_level,
        opt_only=args.opt_only,
        year=args.year,
        months=months,
        dry_run=args.dry_run,
    )

    if args.output_json:
        out_p = Path(args.output_json)
        out_p.parent.mkdir(parents=True, exist_ok=True)
        with open(out_p, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2)
        log.info(f"Saved run telemetry to {args.output_json}")

    if results["status"] != "SUCCESS":
        sys.exit(1)


if __name__ == "__main__":
    main()
