#!/usr/bin/env python3
"""
silver_to_gold.py
~~~~~~~~~~~~~~~~~
Job Spark : Silver → Gold pour les données NYC Yellow Taxi.

Pipeline
--------
1. Lecture de la couche Silver (Delta Lake) et de la table de référence
   des zones taxi (CSV dans Bronze).
2. Jointure des zones (pickup + dropoff) pour obtenir noms et boroughs.
3. Calcul des pré-agrégats métier par zone × heure × jour.
4. Écriture au format Delta dans ``s3a://gold/zone_hourly_metrics/``
   avec ``replaceWhere`` pour idempotence.

Usage
-----
    spark-submit \\
        --master spark://spark-master:7077 \\
        --packages io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4 \\
        silver_to_gold.py --month 2025-01
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

# ── Make spark_jobs package importable ────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from spark_jobs.common.constants import (
    BRONZE_REFERENCE_PATH,
    GOLD_METRICS_PATH,
    SILVER_YELLOW_PATH,
)
from spark_jobs.common.spark_factory import create_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("silver_to_gold")


# ==============================================================================
# Reference data
# ==============================================================================

TAXI_ZONE_SCHEMA = StructType(
    [
        StructField("LocationID", IntegerType(), nullable=False),
        StructField("Borough", StringType(), nullable=True),
        StructField("Zone", StringType(), nullable=True),
        StructField("service_zone", StringType(), nullable=True),
    ]
)


def load_taxi_zones(spark: SparkSession) -> DataFrame:
    """Load taxi zone reference table from Bronze."""
    path = f"{BRONZE_REFERENCE_PATH}/taxi_zone_lookup.csv"
    log.info(f"  Loading taxi zones from {path}")
    return (
        spark.read
        .option("header", "true")
        .schema(TAXI_ZONE_SCHEMA)
        .csv(path)
    )


# ==============================================================================
# Aggregation logic
# ==============================================================================

def build_gold_metrics(
    df_silver: DataFrame,
    df_zones: DataFrame,
) -> DataFrame:
    """Join zones and compute pre-aggregated metrics by zone × hour × day.

    Output columns
    ---------------
    pickup_date, pickup_hour, day_of_week,
    pu_borough, pu_zone, do_borough, do_zone,
    trip_count, total_passengers, avg_trip_distance_miles,
    avg_duration_minutes, total_fare, total_tips, total_revenue,
    avg_speed_mph, airport_trip_count, year, month
    """
    # ── Alias zones for PU and DO joins ──────────────────────────────────
    pu_zones = df_zones.select(
        F.col("LocationID").alias("_pu_loc_id"),
        F.col("Borough").alias("pu_borough"),
        F.col("Zone").alias("pu_zone"),
    )
    do_zones = df_zones.select(
        F.col("LocationID").alias("_do_loc_id"),
        F.col("Borough").alias("do_borough"),
        F.col("Zone").alias("do_zone"),
    )

    # ── Joins ────────────────────────────────────────────────────────────
    df = (
        df_silver
        .join(pu_zones, df_silver["pu_location_id"] == pu_zones["_pu_loc_id"], "left")
        .join(do_zones, df_silver["do_location_id"] == do_zones["_do_loc_id"], "left")
        .drop("_pu_loc_id", "_do_loc_id")
    )

    # ── Add pickup_date ──────────────────────────────────────────────────
    df = df.withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))

    # ── Aggregate ────────────────────────────────────────────────────────
    group_cols = [
        "pickup_date",
        "pickup_hour",
        "day_of_week",
        "pu_borough",
        "pu_zone",
        "do_borough",
        "do_zone",
        "year",
        "month",
    ]

    df_gold = (
        df.groupBy(*group_cols)
        .agg(
            F.count("*").alias("trip_count"),
            F.sum("passenger_count").alias("total_passengers"),
            F.round(F.avg("trip_distance"), 2).alias("avg_trip_distance_miles"),
            F.round(F.avg("duration_minutes"), 2).alias("avg_duration_minutes"),
            F.round(F.sum("fare_amount"), 2).alias("total_fare"),
            F.round(F.sum("tip_amount"), 2).alias("total_tips"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("avg_speed_mph"), 2).alias("avg_speed_mph"),
            F.sum(F.when(F.col("is_airport_trip"), 1).otherwise(0)).alias(
                "airport_trip_count"
            ),
        )
    )

    return df_gold


# ==============================================================================
# Main job
# ==============================================================================

def run(year: int, month: int) -> dict:
    """Execute the Silver → Gold aggregation for one month."""
    t0 = time.monotonic()
    month_str = f"{year:04d}-{month:02d}"
    log.info(f"▶ Starting Silver → Gold for {month_str}")

    spark = create_spark_session(app_name=f"silver-to-gold-{month_str}")

    # ── Read Silver (Delta) — filter on partition ────────────────────────
    log.info(f"  Reading Silver Delta from {SILVER_YELLOW_PATH}")
    df_silver = (
        spark.read
        .format("delta")
        .load(SILVER_YELLOW_PATH)
        .filter((F.col("year") == year) & (F.col("month") == month))
    )
    silver_count = df_silver.count()
    log.info(f"  Silver rows for {month_str}: {silver_count:,}")

    # ── Load reference zones ─────────────────────────────────────────────
    df_zones = load_taxi_zones(spark)
    zone_count = df_zones.count()
    log.info(f"  Taxi zones loaded: {zone_count}")

    # ── Build Gold aggregates ────────────────────────────────────────────
    df_gold = build_gold_metrics(df_silver, df_zones)
    gold_count = df_gold.count()
    log.info(f"  Gold metric rows: {gold_count:,}")

    # ── Write Gold (Delta) with replaceWhere ─────────────────────────────
    log.info(f"  Writing to {GOLD_METRICS_PATH}")
    (
        df_gold
        .write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"year = {year} AND month = {month}")
        .partitionBy("year", "month")
        .save(GOLD_METRICS_PATH)
    )

    duration_s = round(time.monotonic() - t0, 2)
    metrics = {
        "month": month_str,
        "silver_count": silver_count,
        "gold_count": gold_count,
        "duration_s": duration_s,
        "status": "success",
    }
    log.info(f"✅ Silver → Gold completed in {duration_s}s: {metrics}")

    spark.stop()
    return metrics


# ==============================================================================
# CLI
# ==============================================================================

def parse_month(value: str) -> tuple[int, int]:
    try:
        dt = datetime.strptime(value, "%Y-%m")
        return dt.year, dt.month
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Format invalide '{value}'. Attendu : YYYY-MM (ex: 2024-01)."
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Spark job: Silver → Gold (Yellow Taxi zone hourly metrics)",
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
