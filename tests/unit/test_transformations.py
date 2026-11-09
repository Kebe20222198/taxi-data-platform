"""
tests/unit/test_transformations.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Suite complète de tests unitaires pour le module
``spark_jobs.common.transformations``.

Couverture cible > 70 % sur le module de transformation :
  • rename_columns          — renommage snake_case
  • cast_types              — cast DoubleType → IntegerType
  • apply_quality_rules     — chaque règle de rejet individuellement
  • add_derived_columns     — colonnes dérivées
  • deduplicate             — dédoublonnage SHA-256
  • add_partition_columns   — colonnes year/month
"""

from __future__ import annotations

from datetime import datetime

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

from spark_jobs.common.transformations import (
    add_derived_columns,
    add_partition_columns,
    apply_quality_rules,
    cast_types,
    deduplicate,
    rename_columns,
)
from spark_jobs.common.constants import COLUMN_RENAME_MAP


# ==========================================================================
# Helpers
# ==========================================================================

def _ts(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")


def _build_clean_row(spark, **overrides):
    """Build a single-row DataFrame in snake_case + casted types.

    Defaults to a valid January 2024 trip; override any column.
    """
    defaults = {
        "vendor_id": 2,
        "tpep_pickup_datetime": _ts("2024-01-15 08:30:00"),
        "tpep_dropoff_datetime": _ts("2024-01-15 09:00:00"),
        "passenger_count": 1,
        "trip_distance": 5.5,
        "ratecode_id": 1,
        "store_and_fwd_flag": "N",
        "pu_location_id": 132,
        "do_location_id": 236,
        "payment_type": 1,
        "fare_amount": 18.50,
        "extra": 1.00,
        "mta_tax": 0.50,
        "tip_amount": 4.00,
        "tolls_amount": 0.00,
        "improvement_surcharge": 0.30,
        "total_amount": 27.30,
        "congestion_surcharge": 2.50,
        "airport_fee": 1.75,
    }
    defaults.update(overrides)
    schema = StructType([
        StructField("vendor_id", IntegerType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("ratecode_id", IntegerType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("pu_location_id", IntegerType(), True),
        StructField("do_location_id", IntegerType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("airport_fee", DoubleType(), True),
    ])
    return spark.createDataFrame([tuple(defaults.values())], schema=schema)


# ==========================================================================
# 1. rename_columns
# ==========================================================================

class TestRenameColumns:
    """Tests for ``rename_columns``."""

    def test_all_columns_renamed(self, spark, df_raw_valid):
        """Every raw column should be mapped to its snake_case equivalent."""
        df = rename_columns(df_raw_valid)
        expected = set(COLUMN_RENAME_MAP.values())
        assert set(df.columns) == expected

    def test_vendor_id_renamed(self, spark, df_raw_valid):
        df = rename_columns(df_raw_valid)
        assert "vendor_id" in df.columns
        assert "VendorID" not in df.columns

    def test_pu_do_location_renamed(self, spark, df_raw_valid):
        df = rename_columns(df_raw_valid)
        assert "pu_location_id" in df.columns
        assert "do_location_id" in df.columns
        assert "PULocationID" not in df.columns

    def test_airport_fee_renamed(self, spark, df_raw_valid):
        df = rename_columns(df_raw_valid)
        assert "airport_fee" in df.columns
        assert "Airport_fee" not in df.columns

    def test_data_preserved(self, spark, df_raw_valid):
        """Renaming should not alter data values."""
        df = rename_columns(df_raw_valid)
        row = df.first()
        assert row["vendor_id"] == 2
        assert row["pu_location_id"] == 132


# ==========================================================================
# 2. cast_types
# ==========================================================================

class TestCastTypes:
    """Tests for ``cast_types``."""

    def test_integer_cast(self, spark, df_raw_valid):
        df = cast_types(rename_columns(df_raw_valid))
        row = df.first()
        # passenger_count arrives as 1.0 (Double), should become 1 (Int)
        assert row["passenger_count"] == 1
        assert isinstance(row["passenger_count"], int)

    def test_vendor_id_integer(self, spark, df_raw_valid):
        df = cast_types(rename_columns(df_raw_valid))
        assert df.schema["vendor_id"].dataType == IntegerType()

    def test_monetary_stays_double(self, spark, df_raw_valid):
        df = cast_types(rename_columns(df_raw_valid))
        assert df.schema["fare_amount"].dataType == DoubleType()
        assert df.schema["total_amount"].dataType == DoubleType()


# ==========================================================================
# 3. apply_quality_rules
# ==========================================================================

class TestQualityRules:
    """Tests for ``apply_quality_rules`` — one test per rejection rule."""

    # ── R1: invalid duration ─────────────────────────────────────────────

    def test_reject_negative_duration(self, spark):
        """Dropoff before pickup → rejected."""
        df = _build_clean_row(
            spark,
            tpep_pickup_datetime=_ts("2024-01-15 10:00:00"),
            tpep_dropoff_datetime=_ts("2024-01-15 09:30:00"),
        )
        valid, rejected = apply_quality_rules(df, 2024, 1)
        assert valid.count() == 0
        assert rejected.count() == 1
        reason = rejected.first()["reject_reason"]
        assert "invalid_duration" in reason

    def test_reject_zero_duration(self, spark):
        """Same pickup and dropoff time → rejected."""
        ts = _ts("2024-01-15 08:30:00")
        df = _build_clean_row(
            spark,
            tpep_pickup_datetime=ts,
            tpep_dropoff_datetime=ts,
        )
        valid, rejected = apply_quality_rules(df, 2024, 1)
        assert valid.count() == 0
        assert "invalid_duration" in rejected.first()["reject_reason"]

    def test_reject_over_6h_duration(self, spark):
        """7-hour trip → rejected."""
        df = _build_clean_row(
            spark,
            tpep_pickup_datetime=_ts("2024-01-10 06:00:00"),
            tpep_dropoff_datetime=_ts("2024-01-10 13:00:00"),
        )
        valid, rejected = apply_quality_rules(df, 2024, 1)
        assert valid.count() == 0
        assert "invalid_duration" in rejected.first()["reject_reason"]

    def test_accept_exactly_6h(self, spark):
        """Exactly 6 hours → still valid (boundary)."""
        df = _build_clean_row(
            spark,
            tpep_pickup_datetime=_ts("2024-01-10 06:00:00"),
            tpep_dropoff_datetime=_ts("2024-01-10 12:00:00"),
        )
        valid, rejected = apply_quality_rules(df, 2024, 1)
        assert valid.count() == 1
        assert rejected.count() == 0

    # ── R2: invalid distance ─────────────────────────────────────────────

    def test_reject_zero_distance(self, spark):
        df = _build_clean_row(spark, trip_distance=0.0)
        valid, rejected = apply_quality_rules(df, 2024, 1)
        assert valid.count() == 0
        assert "invalid_distance" in rejected.first()["reject_reason"]

    def test_reject_negative_distance(self, spark):
        df = _build_clean_row(spark, trip_distance=-1.0)
        _, rejected = apply_quality_rules(df, 2024, 1)
        assert rejected.count() == 1

    def test_reject_over_100_miles(self, spark):
        df = _build_clean_row(spark, trip_distance=150.0)
        _, rejected = apply_quality_rules(df, 2024, 1)
        assert "invalid_distance" in rejected.first()["reject_reason"]

    def test_accept_exactly_100_miles(self, spark):
        """100 miles is the boundary — should still be valid."""
        df = _build_clean_row(spark, trip_distance=100.0)
        valid, _ = apply_quality_rules(df, 2024, 1)
        assert valid.count() == 1

    # ── R3: invalid total_amount ─────────────────────────────────────────

    def test_reject_negative_total(self, spark):
        df = _build_clean_row(spark, total_amount=-4.20)
        _, rejected = apply_quality_rules(df, 2024, 1)
        assert "invalid_total_amount" in rejected.first()["reject_reason"]

    def test_reject_zero_total(self, spark):
        df = _build_clean_row(spark, total_amount=0.0)
        _, rejected = apply_quality_rules(df, 2024, 1)
        assert "invalid_total_amount" in rejected.first()["reject_reason"]

    # ── R4: pickup outside month ─────────────────────────────────────────

    def test_reject_pickup_next_month(self, spark):
        """February pickup processed as January → rejected."""
        df = _build_clean_row(
            spark,
            tpep_pickup_datetime=_ts("2024-02-01 00:05:00"),
            tpep_dropoff_datetime=_ts("2024-02-01 00:20:00"),
        )
        _, rejected = apply_quality_rules(df, 2024, 1)
        assert "pickup_outside_month" in rejected.first()["reject_reason"]

    def test_reject_pickup_previous_month(self, spark):
        """December pickup processed as January → rejected."""
        df = _build_clean_row(
            spark,
            tpep_pickup_datetime=_ts("2023-12-31 23:50:00"),
            tpep_dropoff_datetime=_ts("2024-01-01 00:10:00"),
        )
        _, rejected = apply_quality_rules(df, 2024, 1)
        assert "pickup_outside_month" in rejected.first()["reject_reason"]

    def test_accept_first_second_of_month(self, spark):
        """Jan 1 00:00:00 → belongs to January."""
        df = _build_clean_row(
            spark,
            tpep_pickup_datetime=_ts("2024-01-01 00:00:00"),
            tpep_dropoff_datetime=_ts("2024-01-01 00:15:00"),
        )
        valid, _ = apply_quality_rules(df, 2024, 1)
        assert valid.count() == 1

    def test_accept_last_second_of_month(self, spark):
        """Jan 31 23:59:59 → still January."""
        df = _build_clean_row(
            spark,
            tpep_pickup_datetime=_ts("2024-01-31 23:59:59"),
            tpep_dropoff_datetime=_ts("2024-02-01 00:15:00"),
        )
        valid, _ = apply_quality_rules(df, 2024, 1)
        assert valid.count() == 1

    # ── R5: invalid zone ID ──────────────────────────────────────────────

    def test_reject_pu_zone_out_of_range(self, spark):
        df = _build_clean_row(spark, pu_location_id=999)
        _, rejected = apply_quality_rules(df, 2024, 1)
        assert "invalid_zone_id" in rejected.first()["reject_reason"]

    def test_reject_do_zone_zero(self, spark):
        df = _build_clean_row(spark, do_location_id=0)
        _, rejected = apply_quality_rules(df, 2024, 1)
        assert "invalid_zone_id" in rejected.first()["reject_reason"]

    def test_reject_null_pu_zone(self, spark):
        df = _build_clean_row(spark, pu_location_id=None)
        _, rejected = apply_quality_rules(df, 2024, 1)
        assert "invalid_zone_id" in rejected.first()["reject_reason"]

    # ── Multiple violations ──────────────────────────────────────────────

    def test_multiple_violations_concatenated(self, spark):
        """A row violating multiple rules gets all reasons joined by '|'."""
        df = _build_clean_row(
            spark,
            trip_distance=0.0,       # R2
            total_amount=-1.0,       # R3
        )
        _, rejected = apply_quality_rules(df, 2024, 1)
        reason = rejected.first()["reject_reason"]
        assert "invalid_distance" in reason
        assert "invalid_total_amount" in reason
        assert "|" in reason

    # ── Full mixed dataset ───────────────────────────────────────────────

    def test_mixed_dataset_counts(self, spark, df_raw_mixed):
        """From the 10-row mixed dataset, exactly 2 valid + 8 rejected."""
        df = cast_types(rename_columns(df_raw_mixed))
        valid, rejected = apply_quality_rules(df, 2024, 1)
        # Row 0 valid, Row 9 valid (duplicate of 0), rest rejected
        assert valid.count() == 2
        assert rejected.count() == 8

    # ── December boundary ────────────────────────────────────────────────

    def test_december_boundary(self, spark):
        """December month boundary should roll over to next year correctly."""
        df = _build_clean_row(
            spark,
            tpep_pickup_datetime=_ts("2024-12-15 10:00:00"),
            tpep_dropoff_datetime=_ts("2024-12-15 10:30:00"),
        )
        valid, rejected = apply_quality_rules(df, 2024, 12)
        assert valid.count() == 1
        assert rejected.count() == 0


# ==========================================================================
# 4. add_derived_columns
# ==========================================================================

class TestDerivedColumns:
    """Tests for ``add_derived_columns``."""

    def test_duration_minutes(self, spark):
        """30-minute trip → duration_minutes = 30.0."""
        df = _build_clean_row(spark)
        df = add_derived_columns(df)
        assert df.first()["duration_minutes"] == 30.0

    def test_avg_speed_mph(self, spark):
        """5.5 miles in 30 min → 11.0 mph."""
        df = _build_clean_row(spark)
        df = add_derived_columns(df)
        assert df.first()["avg_speed_mph"] == 11.0

    def test_pickup_hour(self, spark):
        """Pickup at 08:30 → hour = 8."""
        df = _build_clean_row(spark)
        df = add_derived_columns(df)
        assert df.first()["pickup_hour"] == 8

    def test_day_of_week(self, spark):
        """2024-01-15 is a Monday."""
        df = _build_clean_row(spark)
        df = add_derived_columns(df)
        assert df.first()["day_of_week"] == "Monday"

    def test_is_airport_trip_jfk_pickup(self, spark):
        """PU at JFK (zone 132) → is_airport_trip = True."""
        df = _build_clean_row(spark, pu_location_id=132)
        df = add_derived_columns(df)
        assert df.first()["is_airport_trip"] is True

    def test_is_airport_trip_lga_dropoff(self, spark):
        """DO at LaGuardia (zone 138) → is_airport_trip = True."""
        df = _build_clean_row(spark, pu_location_id=100, do_location_id=138)
        df = add_derived_columns(df)
        assert df.first()["is_airport_trip"] is True

    def test_is_airport_trip_newark(self, spark):
        """DO at Newark EWR (zone 1) → is_airport_trip = True."""
        df = _build_clean_row(spark, pu_location_id=100, do_location_id=1)
        df = add_derived_columns(df)
        assert df.first()["is_airport_trip"] is True

    def test_not_airport_trip(self, spark):
        """Neither PU nor DO at an airport → is_airport_trip = False."""
        df = _build_clean_row(spark, pu_location_id=100, do_location_id=200)
        df = add_derived_columns(df)
        assert df.first()["is_airport_trip"] is False

    def test_all_derived_columns_present(self, spark):
        """All 5 derived columns should exist in the output."""
        df = _build_clean_row(spark)
        df = add_derived_columns(df)
        expected = {
            "duration_minutes",
            "avg_speed_mph",
            "pickup_hour",
            "day_of_week",
            "is_airport_trip",
        }
        assert expected.issubset(set(df.columns))


# ==========================================================================
# 5. deduplicate
# ==========================================================================

class TestDeduplicate:
    """Tests for ``deduplicate``."""

    def test_removes_exact_duplicates(self, spark, df_raw_mixed):
        """Rows 0 and 9 are identical — dedup should keep only one."""
        df = cast_types(rename_columns(df_raw_mixed))
        valid, _ = apply_quality_rules(df, 2024, 1)
        # valid has 2 rows (row 0 and row 9, which are duplicates)
        assert valid.count() == 2

        df_deduped = deduplicate(valid)
        assert df_deduped.count() == 1

    def test_trip_hash_column_added(self, spark):
        df = _build_clean_row(spark)
        df = deduplicate(df)
        assert "trip_hash" in df.columns

    def test_hash_is_sha256(self, spark):
        """trip_hash should be 64 hex characters (SHA-256)."""
        df = _build_clean_row(spark)
        df = deduplicate(df)
        hash_val = df.first()["trip_hash"]
        assert len(hash_val) == 64
        assert all(c in "0123456789abcdef" for c in hash_val)

    def test_different_trips_different_hashes(self, spark):
        """Two distinct trips should have different hashes."""
        df1 = _build_clean_row(spark, trip_distance=5.5)
        df2 = _build_clean_row(spark, trip_distance=10.0)
        df = df1.union(df2)
        df = deduplicate(df)
        assert df.count() == 2
        hashes = [row["trip_hash"] for row in df.collect()]
        assert hashes[0] != hashes[1]


# ==========================================================================
# 6. add_partition_columns
# ==========================================================================

class TestPartitionColumns:
    """Tests for ``add_partition_columns``."""

    def test_year_month_added(self, spark):
        df = _build_clean_row(spark)
        df = add_partition_columns(df, 2024, 1)
        row = df.first()
        assert row["year"] == 2024
        assert row["month"] == 1

    def test_partition_types(self, spark):
        df = _build_clean_row(spark)
        df = add_partition_columns(df, 2024, 1)
        assert df.schema["year"].dataType == IntegerType()
        assert df.schema["month"].dataType == IntegerType()


# ==========================================================================
# 7. End-to-end pipeline (without I/O)
# ==========================================================================

class TestEndToEnd:
    """Integration-style test: full transformation chain on sample data."""

    def test_full_pipeline_on_mixed_data(self, spark, df_raw_mixed):
        """Run the complete Bronze→Silver transformation chain."""
        # rename → cast → quality → enrich → dedup → partition
        df = rename_columns(df_raw_mixed)
        df = cast_types(df)
        valid, rejected = apply_quality_rules(df, 2024, 1)
        valid = add_derived_columns(valid)
        valid = deduplicate(valid)
        valid = add_partition_columns(valid, 2024, 1)

        # Should end up with 1 row (2 valid, but duplicated → 1)
        assert valid.count() == 1

        # Should have all expected columns
        row = valid.first()
        assert row["year"] == 2024
        assert row["month"] == 1
        assert row["duration_minutes"] == 30.0
        assert row["is_airport_trip"] is True  # PU at JFK (132)
        assert row["trip_hash"] is not None

        # Rejected should contain reject_reason for all
        assert rejected.count() == 8
        assert "reject_reason" in rejected.columns
