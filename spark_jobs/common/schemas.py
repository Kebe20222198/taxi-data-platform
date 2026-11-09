"""
spark_jobs.common.schemas
~~~~~~~~~~~~~~~~~~~~~~~~~
Schéma explicite strict (StructType) pour les fichiers Parquet
NYC Yellow Taxi Trip Records.  **Aucune inférence de schéma.**

Les types correspondent à ce que publie réellement le NYC TLC
dans ses fichiers Parquet (2022-2025).  Certaines colonnes qui
sont sémantiquement des entiers (passenger_count, RatecodeID)
arrivent comme DoubleType dans le Parquet source — le cast vers
Integer se fait dans la couche transformation.
"""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ==============================================================================
# Raw schema — miroir fidèle du fichier Parquet TLC
# ==============================================================================
YELLOW_TAXI_RAW_SCHEMA = StructType(
    [
        StructField("VendorID", IntegerType(), nullable=True),
        StructField("tpep_pickup_datetime", TimestampType(), nullable=True),
        StructField("tpep_dropoff_datetime", TimestampType(), nullable=True),
        StructField("passenger_count", LongType(), nullable=True),
        StructField("trip_distance", DoubleType(), nullable=True),
        StructField("RatecodeID", LongType(), nullable=True),
        StructField("store_and_fwd_flag", StringType(), nullable=True),
        StructField("PULocationID", IntegerType(), nullable=True),
        StructField("DOLocationID", IntegerType(), nullable=True),
        StructField("payment_type", LongType(), nullable=True),
        StructField("fare_amount", DoubleType(), nullable=True),
        StructField("extra", DoubleType(), nullable=True),
        StructField("mta_tax", DoubleType(), nullable=True),
        StructField("tip_amount", DoubleType(), nullable=True),
        StructField("tolls_amount", DoubleType(), nullable=True),
        StructField("improvement_surcharge", DoubleType(), nullable=True),
        StructField("total_amount", DoubleType(), nullable=True),
        StructField("congestion_surcharge", DoubleType(), nullable=True),
        StructField("Airport_fee", DoubleType(), nullable=True),
        StructField("cbd_congestion_fee", DoubleType(), nullable=True),
    ]
)

# ==============================================================================
# Silver schema — après rename + cast (pour documentation / validation)
# ==============================================================================
YELLOW_TAXI_SILVER_SCHEMA = StructType(
    [
        StructField("vendor_id", IntegerType(), nullable=True),
        StructField("tpep_pickup_datetime", TimestampType(), nullable=True),
        StructField("tpep_dropoff_datetime", TimestampType(), nullable=True),
        StructField("passenger_count", IntegerType(), nullable=True),
        StructField("trip_distance", DoubleType(), nullable=True),
        StructField("ratecode_id", IntegerType(), nullable=True),
        StructField("store_and_fwd_flag", StringType(), nullable=True),
        StructField("pu_location_id", IntegerType(), nullable=True),
        StructField("do_location_id", IntegerType(), nullable=True),
        StructField("payment_type", IntegerType(), nullable=True),
        StructField("fare_amount", DoubleType(), nullable=True),
        StructField("extra", DoubleType(), nullable=True),
        StructField("mta_tax", DoubleType(), nullable=True),
        StructField("tip_amount", DoubleType(), nullable=True),
        StructField("tolls_amount", DoubleType(), nullable=True),
        StructField("improvement_surcharge", DoubleType(), nullable=True),
        StructField("total_amount", DoubleType(), nullable=True),
        StructField("congestion_surcharge", DoubleType(), nullable=True),
        StructField("airport_fee", DoubleType(), nullable=True),
        # ── Derived columns ──────────────────────────────────────────
        StructField("duration_minutes", DoubleType(), nullable=True),
        StructField("avg_speed_mph", DoubleType(), nullable=True),
        StructField("pickup_hour", IntegerType(), nullable=True),
        StructField("day_of_week", StringType(), nullable=True),
        StructField("is_airport_trip", BooleanType(), nullable=True),
        StructField("trip_hash", StringType(), nullable=False),
        # ── Partition columns ────────────────────────────────────────
        StructField("year", IntegerType(), nullable=False),
        StructField("month", IntegerType(), nullable=False),
    ]
)
