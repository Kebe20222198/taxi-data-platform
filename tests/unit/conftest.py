"""
Unit test fixtures — sample DataFrames for Yellow Taxi transformations.
"""

from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# ── Minimal raw schema matching the real Parquet structure ───────────────────
RAW_SCHEMA = StructType(
    [
        StructField("VendorID", LongType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", DoubleType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("RatecodeID", DoubleType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("PULocationID", LongType(), True),
        StructField("DOLocationID", LongType(), True),
        StructField("payment_type", LongType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("Airport_fee", DoubleType(), True),
    ]
)


def _ts(s: str) -> datetime:
    """Shortcut to build a datetime from 'YYYY-MM-DD HH:MM:SS'."""
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")


@pytest.fixture()
def df_raw_valid(spark: SparkSession):
    """A single perfectly valid trip in January 2024."""
    data = [
        (
            2,                                        # VendorID
            _ts("2024-01-15 08:30:00"),               # pickup
            _ts("2024-01-15 09:00:00"),               # dropoff (30 min)
            1.0,                                      # passenger_count
            5.5,                                      # trip_distance
            1.0,                                      # RatecodeID
            "N",                                      # store_and_fwd_flag
            132,                                      # PULocationID (JFK)
            236,                                      # DOLocationID
            1,                                        # payment_type
            18.50,                                    # fare_amount
            1.00,                                     # extra
            0.50,                                     # mta_tax
            4.00,                                     # tip_amount
            0.00,                                     # tolls_amount
            0.30,                                     # improvement_surcharge
            27.30,                                    # total_amount
            2.50,                                     # congestion_surcharge
            1.75,                                     # Airport_fee
        ),
    ]
    return spark.createDataFrame(data, schema=RAW_SCHEMA)


@pytest.fixture()
def df_raw_mixed(spark: SparkSession):
    """Mix of valid and invalid trips covering all rejection rules."""
    data = [
        # Row 0 — VALID: normal 30-min trip in Jan 2024
        (2, _ts("2024-01-15 08:30:00"), _ts("2024-01-15 09:00:00"),
         1.0, 5.5, 1.0, "N", 132, 236, 1, 18.50, 1.0, 0.5, 4.0, 0.0, 0.3, 27.30, 2.5, 1.75),
        # Row 1 — REJECT: negative duration (dropoff before pickup)
        (2, _ts("2024-01-15 10:00:00"), _ts("2024-01-15 09:30:00"),
         1.0, 3.0, 1.0, "N", 161, 237, 1, 12.00, 0.0, 0.5, 2.0, 0.0, 0.3, 15.80, 2.5, 0.0),
        # Row 2 — REJECT: duration > 6 hours (7 hours)
        (1, _ts("2024-01-10 06:00:00"), _ts("2024-01-10 13:00:00"),
         2.0, 25.0, 1.0, "N", 138, 132, 1, 80.00, 0.0, 0.5, 10.0, 6.55, 0.3, 98.35, 2.5, 1.75),
        # Row 3 — REJECT: distance <= 0
        (2, _ts("2024-01-20 14:00:00"), _ts("2024-01-20 14:15:00"),
         1.0, 0.0, 1.0, "N", 48, 68, 1, 8.00, 0.0, 0.5, 1.50, 0.0, 0.3, 10.30, 2.5, 0.0),
        # Row 4 — REJECT: distance > 100 miles
        (1, _ts("2024-01-20 07:00:00"), _ts("2024-01-20 08:00:00"),
         1.0, 150.0, 1.0, "N", 132, 265, 1, 200.00, 0.0, 0.5, 40.0, 15.0, 0.3, 256.80, 2.5, 1.75),
        # Row 5 — REJECT: total_amount <= 0
        (2, _ts("2024-01-22 18:00:00"), _ts("2024-01-22 18:20:00"),
         1.0, 2.5, 1.0, "N", 79, 113, 1, -5.00, 0.0, 0.5, 0.0, 0.0, 0.3, -4.20, 2.5, 0.0),
        # Row 6 — REJECT: pickup outside month (February)
        (2, _ts("2024-02-01 00:05:00"), _ts("2024-02-01 00:20:00"),
         1.0, 3.0, 1.0, "N", 161, 237, 1, 12.00, 0.0, 0.5, 2.50, 0.0, 0.3, 16.30, 2.5, 0.0),
        # Row 7 — REJECT: invalid PU zone (999)
        (1, _ts("2024-01-25 12:00:00"), _ts("2024-01-25 12:30:00"),
         2.0, 8.0, 1.0, "N", 999, 132, 1, 30.00, 0.0, 0.5, 6.0, 0.0, 0.3, 37.80, 2.5, 1.75),
        # Row 8 — REJECT: invalid DO zone (0)
        (1, _ts("2024-01-25 13:00:00"), _ts("2024-01-25 13:45:00"),
         1.0, 10.0, 1.0, "Y", 138, 0, 2, 35.00, 0.0, 0.5, 0.0, 0.0, 0.3, 36.80, 2.5, 1.75),
        # Row 9 — VALID: another good trip (for dedup test — same as row 0)
        (2, _ts("2024-01-15 08:30:00"), _ts("2024-01-15 09:00:00"),
         1.0, 5.5, 1.0, "N", 132, 236, 1, 18.50, 1.0, 0.5, 4.0, 0.0, 0.3, 27.30, 2.5, 1.75),
    ]
    return spark.createDataFrame(data, schema=RAW_SCHEMA)
