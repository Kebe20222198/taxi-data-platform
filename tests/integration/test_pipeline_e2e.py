"""
tests/integration/test_pipeline_e2e.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Test d'intégration end-to-end du pipeline Spark :
  1. Données brutes Parquet simulées (Bronze)
  2. Ingestion + Nettoyage + Règles Qualité + Quarantaine (Bronze → Silver)
  3. Dédoublonnage SHA-256 + Enrichissement
  4. Jointure des tables de référence Zones + Agrégation Gold (Silver → Gold)
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark_jobs.common.schemas import YELLOW_TAXI_RAW_SCHEMA
from spark_jobs.common.transformations import (
    add_derived_columns,
    add_partition_columns,
    apply_quality_rules,
    cast_types,
    deduplicate,
    rename_columns,
)
from spark_jobs.silver_to_gold import TAXI_ZONE_SCHEMA, build_gold_metrics


@pytest.fixture
def sample_zones_df(spark: SparkSession):
    """Fixture providing a minimal taxi zone lookup table."""
    zones_data = [
        (132, "Queens", "JFK Airport", "Airports"),
        (138, "Queens", "LaGuardia Airport", "Airports"),
        (161, "Manhattan", "Midtown Center", "Yellow Zone"),
        (230, "Manhattan", "Times Sq/Theatre District", "Yellow Zone"),
    ]
    return spark.createDataFrame(zones_data, schema=TAXI_ZONE_SCHEMA)


def test_end_to_end_pipeline_simulation(
    spark: SparkSession,
    sample_zones_df,
    tmp_path: Path,
):
    """Simulate complete Bronze → Silver → Gold pipeline on disk."""
    # ── 1. Create simulated bronze parquet data ──────────────────────────
    bronze_dir = tmp_path / "bronze" / "yellow" / "year=2024" / "month=01"
    bronze_dir.mkdir(parents=True)

    raw_records = [
        # Trip 1: Normal valid trip JFK -> Midtown
        (
            2,
            datetime(2024, 1, 15, 10, 0, 0),
            datetime(2024, 1, 15, 10, 45, 0),
            1.0,
            18.5,
            1.0,
            "N",
            132,
            161,
            1,
            52.0,
            0.0,
            0.5,
            10.0,
            6.55,
            0.3,
            70.35,
            2.5,
            0.0,
        ),
        # Trip 2: Normal valid trip Times Square -> Midtown
        (
            1,
            datetime(2024, 1, 15, 14, 0, 0),
            datetime(2024, 1, 15, 14, 15, 0),
            2.0,
            1.2,
            1.0,
            "N",
            230,
            161,
            1,
            9.0,
            0.0,
            0.5,
            2.5,
            0.0,
            0.3,
            12.3,
            2.5,
            0.0,
        ),
        # Trip 3: Exact duplicate of Trip 2
        (
            1,
            datetime(2024, 1, 15, 14, 0, 0),
            datetime(2024, 1, 15, 14, 15, 0),
            2.0,
            1.2,
            1.0,
            "N",
            230,
            161,
            1,
            9.0,
            0.0,
            0.5,
            2.5,
            0.0,
            0.3,
            12.3,
            2.5,
            0.0,
        ),
        # Trip 4: Invalid negative amount and 0 distance (to be quarantined)
        (
            1,
            datetime(2024, 1, 15, 15, 0, 0),
            datetime(2024, 1, 15, 15, 5, 0),
            1.0,
            0.0,
            1.0,
            "N",
            161,
            161,
            2,
            -10.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            -10.0,
            0.0,
            0.0,
        ),
    ]

    df_raw = spark.createDataFrame(raw_records, schema=YELLOW_TAXI_RAW_SCHEMA)
    bronze_file = str(bronze_dir / "data.parquet")
    df_raw.write.parquet(bronze_file)

    # ── 2. Read bronze with explicit schema ──────────────────────────────
    df_read = spark.read.schema(YELLOW_TAXI_RAW_SCHEMA).parquet(bronze_file)
    assert df_read.count() == 4

    # ── 3. Bronze -> Silver transformations ──────────────────────────────
    df_cleaned = cast_types(rename_columns(df_read))
    df_valid, df_quarantine = apply_quality_rules(df_cleaned, 2024, 1)

    assert df_quarantine.count() == 1
    assert "reject_reason" in df_quarantine.columns
    assert df_valid.count() == 3

    # Enrich + Deduplicate
    df_enriched = add_derived_columns(df_valid)
    assert "duration_minutes" in df_enriched.columns
    assert "avg_speed_mph" in df_enriched.columns
    assert "is_airport_trip" in df_enriched.columns

    df_deduped = deduplicate(df_enriched)
    # Trip 3 duplicate should have been dropped: 3 -> 2
    assert df_deduped.count() == 2

    df_silver = add_partition_columns(df_deduped, 2024, 1)
    silver_out_dir = str(tmp_path / "silver" / "yellow")
    df_silver.write.partitionBy("year", "month").parquet(silver_out_dir)

    # ── 4. Silver -> Gold aggregations ───────────────────────────────────
    df_silver_read = spark.read.parquet(silver_out_dir)
    df_gold = build_gold_metrics(df_silver_read, sample_zones_df)

    assert df_gold.count() == 2
    gold_rows = {row["pu_zone"]: row for row in df_gold.collect()}

    assert "JFK Airport" in gold_rows
    assert gold_rows["JFK Airport"]["airport_trip_count"] == 1
    assert gold_rows["JFK Airport"]["trip_count"] == 1
    assert gold_rows["JFK Airport"]["total_fare"] == 52.0

    assert "Times Sq/Theatre District" in gold_rows
    assert gold_rows["Times Sq/Theatre District"]["airport_trip_count"] == 0
    assert gold_rows["Times Sq/Theatre District"]["trip_count"] == 1
