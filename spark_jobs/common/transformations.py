"""
spark_jobs.common.transformations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Fonctions pures de nettoyage, validation et enrichissement des
données Yellow Taxi.  Chaque fonction prend un DataFrame en entrée
et retourne un (ou deux) DataFrame(s) — aucun effet de bord, aucun
accès réseau, aucune écriture.  100 % testable via ``pytest``.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

from spark_jobs.common.constants import (
    AIRPORT_ZONE_IDS,
    COLUMN_RENAME_MAP,
    DEDUP_KEY_COLUMNS,
    MAX_TRIP_DURATION_SECONDS,
    MAX_TRIP_DISTANCE_MILES,
    VALID_ZONE_IDS,
)


# ==============================================================================
# 1. Rename columns → snake_case
# ==============================================================================

def rename_columns(df: DataFrame) -> DataFrame:
    """Rename all columns from raw Parquet names to snake_case.

    Uses the explicit mapping from ``COLUMN_RENAME_MAP``.  Columns not
    present in the map are left unchanged (forward-compatibility).
    """
    for old_name, new_name in COLUMN_RENAME_MAP.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)
    return df


# ==============================================================================
# 2. Cast types (raw doubles → proper ints, timestamps stay)
# ==============================================================================

def cast_types(df: DataFrame) -> DataFrame:
    """Cast raw DoubleType columns to their semantic types.

    - ``passenger_count``, ``ratecode_id``, ``payment_type``,
      ``vendor_id``, ``pu_location_id``, ``do_location_id`` → IntegerType
    - Monetary columns stay as DoubleType (already correct).
    - Timestamps stay as TimestampType.
    """
    int_columns = [
        "vendor_id",
        "passenger_count",
        "ratecode_id",
        "pu_location_id",
        "do_location_id",
        "payment_type",
    ]
    for col_name in int_columns:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(IntegerType()))
    return df


# ==============================================================================
# 3. Quality rules — split into valid / rejected
# ==============================================================================

def apply_quality_rules(
    df: DataFrame,
    year: int,
    month: int,
    valid_zone_ids: set[int] | None = None,
) -> tuple[DataFrame, DataFrame]:
    """Apply data-quality rules and split the DataFrame.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame with snake_case columns and casted types.
    year, month : int
        The processed month — rows with pickup outside this month are
        rejected.
    valid_zone_ids : set[int] | None
        Set of valid taxi zone IDs.  Defaults to ``VALID_ZONE_IDS``
        (1–265).

    Returns
    -------
    (valid_df, rejected_df)
        ``rejected_df`` contains an extra column ``reject_reason``
        listing all violated rules separated by ``|``.
    """
    if valid_zone_ids is None:
        valid_zone_ids = VALID_ZONE_IDS

    valid_zone_list = sorted(valid_zone_ids)

    # ── Month boundaries ─────────────────────────────────────────────────
    month_start = f"{year:04d}-{month:02d}-01"
    if month == 12:
        month_end = f"{year + 1:04d}-01-01"
    else:
        month_end = f"{year:04d}-{month + 1:02d}-01"

    # ── Compute trip duration (seconds) ──────────────────────────────────
    df = df.withColumn(
        "_duration_s",
        F.unix_timestamp("tpep_dropoff_datetime")
        - F.unix_timestamp("tpep_pickup_datetime"),
    )

    # ── Rule flags (each returns a string reason when violated, else null)
    rule_exprs = [
        # R1: duration <= 0 OR > 6 h
        F.when(
            F.col("_duration_s").isNull()
            | (F.col("_duration_s") <= 0)
            | (F.col("_duration_s") > MAX_TRIP_DURATION_SECONDS),
            F.lit("invalid_duration"),
        ),
        # R2: distance <= 0 OR > 100 mi
        F.when(
            F.col("trip_distance").isNull()
            | (F.col("trip_distance") <= 0)
            | (F.col("trip_distance") > MAX_TRIP_DISTANCE_MILES),
            F.lit("invalid_distance"),
        ),
        # R3: total_amount <= 0
        F.when(
            F.col("total_amount").isNull() | (F.col("total_amount") <= 0),
            F.lit("invalid_total_amount"),
        ),
        # R4: pickup outside processed month
        F.when(
            F.col("tpep_pickup_datetime").isNull()
            | (F.col("tpep_pickup_datetime") < F.lit(month_start).cast("timestamp"))
            | (F.col("tpep_pickup_datetime") >= F.lit(month_end).cast("timestamp")),
            F.lit("pickup_outside_month"),
        ),
        # R5: PU or DO zone not in reference
        F.when(
            F.col("pu_location_id").isNull()
            | F.col("do_location_id").isNull()
            | ~F.col("pu_location_id").isin(valid_zone_list)
            | ~F.col("do_location_id").isin(valid_zone_list),
            F.lit("invalid_zone_id"),
        ),
    ]

    # Concatenate non-null reasons with pipe separator
    df = df.withColumn("reject_reason", F.concat_ws("|", *rule_exprs))

    # ── Split ────────────────────────────────────────────────────────────
    rejected_df = (
        df.filter(F.col("reject_reason") != "")
        .drop("_duration_s")
    )
    valid_df = (
        df.filter(F.col("reject_reason") == "")
        .drop("reject_reason", "_duration_s")
    )

    return valid_df, rejected_df


# ==============================================================================
# 4. Derived / enrichment columns
# ==============================================================================

def add_derived_columns(df: DataFrame) -> DataFrame:
    """Add business-level derived columns to valid trip data.

    New columns
    -----------
    duration_minutes : double
        Trip duration in minutes.
    avg_speed_mph : double
        Average speed (mph).  Null when duration is zero.
    pickup_hour : int
        Hour of day of pickup (0-23).
    day_of_week : str
        English day name (Monday … Sunday).
    is_airport_trip : bool
        ``True`` if PU or DO zone is JFK, LaGuardia or Newark.
    """
    airport_zone_list = sorted(AIRPORT_ZONE_IDS)

    df = (
        df
        # Duration in minutes
        .withColumn(
            "duration_minutes",
            (
                (
                    F.unix_timestamp("tpep_dropoff_datetime")
                    - F.unix_timestamp("tpep_pickup_datetime")
                )
                / 60.0
            ).cast(DoubleType()),
        )
        # Average speed (mph)  — guard against division by zero
        .withColumn(
            "avg_speed_mph",
            F.when(
                F.col("duration_minutes") > 0,
                F.round(
                    F.col("trip_distance") / (F.col("duration_minutes") / 60.0), 2
                ),
            ),
        )
        # Hour of pickup
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime").cast(IntegerType()))
        # Day of week
        .withColumn("day_of_week", F.date_format("tpep_pickup_datetime", "EEEE"))
        # Airport trip flag
        .withColumn(
            "is_airport_trip",
            F.col("pu_location_id").isin(airport_zone_list)
            | F.col("do_location_id").isin(airport_zone_list),
        )
    )
    return df


# ==============================================================================
# 5. Deduplication via SHA-256 hash
# ==============================================================================

def deduplicate(df: DataFrame) -> DataFrame:
    """Deduplicate trips based on a SHA-256 hash of key columns.

    Adds a ``trip_hash`` column and drops exact duplicates.
    """
    hash_expr = F.sha2(
        F.concat_ws(
            "|",
            *[F.col(c).cast("string") for c in DEDUP_KEY_COLUMNS],
        ),
        256,
    )
    df = df.withColumn("trip_hash", hash_expr)
    df = df.dropDuplicates(["trip_hash"])
    return df


# ==============================================================================
# 6. Add partition columns (convenience)
# ==============================================================================

def add_partition_columns(df: DataFrame, year: int, month: int) -> DataFrame:
    """Add ``year`` and ``month`` integer columns for partitioned writes."""
    return (
        df
        .withColumn("year", F.lit(year).cast(IntegerType()))
        .withColumn("month", F.lit(month).cast(IntegerType()))
    )
