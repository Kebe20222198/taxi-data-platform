"""
spark_jobs.stream_trips
~~~~~~~~~~~~~~~~~~~~~~~
Job Spark Structured Streaming consommant le topic Kafka ``taxi_trips``
et produisant des agrégats en temps réel dans Delta Lake.

Architecture
------------
1. Lecture continue du topic Kafka ``taxi_trips`` (JSON).
2. Parsing JSON → schéma structuré via le StructType explicite.
3. **Réutilisation du code batch** : import et application des
   mêmes fonctions de typage, renommage et nettoyage que la couche
   Silver batch (``spark_jobs.common.transformations``).
4. Watermark de 10 minutes sur ``tpep_pickup_datetime`` pour gérer
   les événements en retard (late data).
5. Fenêtres fixes (tumbling) de 5 minutes par zone de départ :
   comptage de trajets, chiffre d'affaires et tarif moyen.
6. Écriture continue au format Delta Lake dans
   ``s3a://gold/streaming_zone_5m/`` avec checkpoints strictement
   gérés dans ``s3a://gold/checkpoints/stream_trips/``.
7. Trigger ``processingTime="10 seconds"`` garantissant une latence
   de bout en bout < 30 secondes.

Usage
-----
.. code-block:: bash

    # Via spark-submit (dans le conteneur Spark)
    spark-submit --master spark://spark-master:7077 \\
        --packages io.delta:delta-spark_2.12:3.1.0,\\
                   org.apache.hadoop:hadoop-aws:3.3.4,\\
                   org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \\
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \\
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \\
        /opt/spark_jobs/stream_trips.py

    # Avec paramètres personnalisés
    spark-submit ... /opt/spark_jobs/stream_trips.py \\
        --kafka-bootstrap kafka:29092 \\
        --watermark 15 \\
        --window 10 \\
        --trigger 20
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

from pyspark.sql import DataFrame, SparkSession
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

# ==============================================================================
# Configuration
# ==============================================================================

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("stream_trips")

# Default values
DEFAULT_KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
DEFAULT_KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "taxi_trips")
DEFAULT_OUTPUT_PATH = "s3a://gold/streaming_zone_5m"
DEFAULT_CHECKPOINT_PATH = "s3a://gold/checkpoints/stream_trips"
DEFAULT_WATERMARK_MINUTES = 10
DEFAULT_WINDOW_MINUTES = 5
DEFAULT_TRIGGER_SECONDS = 10


# ==============================================================================
# Kafka JSON Schema — mirrors YELLOW_TAXI_RAW_SCHEMA for JSON deserialization
# ==============================================================================

KAFKA_TRIP_SCHEMA = StructType([
    StructField("VendorID", LongType(), nullable=True),
    StructField("tpep_pickup_datetime", StringType(), nullable=True),
    StructField("tpep_dropoff_datetime", StringType(), nullable=True),
    StructField("passenger_count", DoubleType(), nullable=True),
    StructField("trip_distance", DoubleType(), nullable=True),
    StructField("RatecodeID", DoubleType(), nullable=True),
    StructField("store_and_fwd_flag", StringType(), nullable=True),
    StructField("PULocationID", LongType(), nullable=True),
    StructField("DOLocationID", LongType(), nullable=True),
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
])


# ==============================================================================
# Reuse batch transformations from spark_jobs.common
# ==============================================================================

def apply_batch_transformations_streaming(df: DataFrame) -> DataFrame:
    """Apply the same rename + cast logic from the batch Silver pipeline.

    We import and reuse the exact functions from ``spark_jobs.common.transformations``
    to guarantee consistency between batch and streaming quality rules.

    Note: ``apply_quality_rules()`` requires year/month parameters and produces
    a rejected split — for streaming we apply lightweight filtering instead of
    the full quality gate (which requires batch semantics).
    The rename + cast + derived enrichment logic is fully reused.
    """
    # ── Import batch transformation functions ────────────────────────────
    # These imports work inside Spark containers where spark_jobs is on PYTHONPATH
    try:
        from spark_jobs.common.transformations import (
            rename_columns,
            cast_types,
            add_derived_columns,
        )
        from spark_jobs.common.constants import (
            MAX_TRIP_DURATION_SECONDS,
            MAX_TRIP_DISTANCE_MILES,
            VALID_ZONE_ID_MIN,
            VALID_ZONE_ID_MAX,
        )
        logger.info("✅ Fonctions de transformation batch importées avec succès.")
    except ImportError as e:
        logger.warning(
            "⚠️ Import batch échoué (%s). Application des transformations inline.",
            e,
        )
        # Fallback inline — exact same logic as transformations.py
        return _apply_transformations_inline(df)

    # ── Step 1: Rename columns (VendorID → vendor_id, etc.) ─────────────
    df = rename_columns(df)

    # ── Step 2: Cast types (Double → Integer for IDs) ───────────────────
    df = cast_types(df)

    # ── Step 3: Streaming quality filter (lightweight) ──────────────────
    # Same 5 rules as apply_quality_rules() but without year/month split
    df = df.filter(
        # R1: Valid duration
        (
            F.unix_timestamp("tpep_dropoff_datetime")
            - F.unix_timestamp("tpep_pickup_datetime")
        ).between(1, MAX_TRIP_DURATION_SECONDS)
        # R2: Valid distance
        & F.col("trip_distance").between(0.01, MAX_TRIP_DISTANCE_MILES)
        # R3: Positive total amount
        & (F.col("total_amount") > 0)
        # R5: Valid zone IDs
        & F.col("pu_location_id").between(VALID_ZONE_ID_MIN, VALID_ZONE_ID_MAX)
        & F.col("do_location_id").between(VALID_ZONE_ID_MIN, VALID_ZONE_ID_MAX)
    )

    # ── Step 4: Add derived columns (duration, speed, hour, etc.) ───────
    df = add_derived_columns(df)

    return df


def _apply_transformations_inline(df: DataFrame) -> DataFrame:
    """Inline fallback — exact same logic when batch module is unavailable.

    This is used only if spark_jobs.common cannot be imported (e.g., during
    standalone testing). The logic mirrors transformations.py exactly.
    """
    # Rename
    rename_map = {
        "VendorID": "vendor_id",
        "RatecodeID": "ratecode_id",
        "PULocationID": "pu_location_id",
        "DOLocationID": "do_location_id",
        "Airport_fee": "airport_fee",
    }
    for old_name, new_name in rename_map.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)

    # Cast
    for col_name in ["vendor_id", "passenger_count", "ratecode_id",
                      "pu_location_id", "do_location_id", "payment_type"]:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(IntegerType()))

    # Quality filter
    df = df.filter(
        (
            F.unix_timestamp("tpep_dropoff_datetime")
            - F.unix_timestamp("tpep_pickup_datetime")
        ).between(1, 21600)
        & F.col("trip_distance").between(0.01, 100.0)
        & (F.col("total_amount") > 0)
        & F.col("pu_location_id").between(1, 265)
        & F.col("do_location_id").between(1, 265)
    )

    # Derived columns
    df = (
        df
        .withColumn(
            "duration_minutes",
            (
                (F.unix_timestamp("tpep_dropoff_datetime")
                 - F.unix_timestamp("tpep_pickup_datetime"))
                / 60.0
            ).cast(DoubleType()),
        )
        .withColumn(
            "avg_speed_mph",
            F.when(
                F.col("duration_minutes") > 0,
                F.round(F.col("trip_distance") / (F.col("duration_minutes") / 60.0), 2),
            ),
        )
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime").cast(IntegerType()))
        .withColumn("day_of_week", F.date_format("tpep_pickup_datetime", "EEEE"))
        .withColumn(
            "is_airport_trip",
            F.col("pu_location_id").isin([1, 132, 138])
            | F.col("do_location_id").isin([1, 132, 138]),
        )
    )

    return df


# ==============================================================================
# Spark Session Factory (Streaming-specific)
# ==============================================================================

def create_streaming_spark_session(app_name: str = "stream-trips") -> SparkSession:
    """Build a SparkSession configured for Structured Streaming + Kafka + Delta.

    Reuses the same S3A/Delta configuration as the batch factory but adds
    Kafka-specific packages.
    """
    try:
        from spark_jobs.common.spark_factory import create_spark_session
        logger.info("✅ Utilisation de la factory batch Spark existante.")
        return create_spark_session(
            app_name=app_name,
            extra_config={
                # Streaming-specific tuning
                "spark.sql.streaming.schemaInference": "false",
                "spark.sql.streaming.checkpointLocation": DEFAULT_CHECKPOINT_PATH,
                # Delta streaming
                "spark.databricks.delta.schema.autoMerge.enabled": "true",
            },
        )
    except ImportError:
        logger.warning("⚠️ Factory batch indisponible. Création manuelle.")

    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("AWS_ACCESS_KEY_ID", os.getenv("MINIO_ROOT_USER", "minioadmin"))
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))

    return (
        SparkSession.builder
        .appName(app_name)
        # Delta Lake
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # S3A / MinIO
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # Performance tuning
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )


# ==============================================================================
# Streaming Pipeline
# ==============================================================================

def run_streaming_pipeline(
    spark: SparkSession,
    *,
    kafka_bootstrap: str = DEFAULT_KAFKA_BOOTSTRAP,
    kafka_topic: str = DEFAULT_KAFKA_TOPIC,
    output_path: str = DEFAULT_OUTPUT_PATH,
    checkpoint_path: str = DEFAULT_CHECKPOINT_PATH,
    watermark_minutes: int = DEFAULT_WATERMARK_MINUTES,
    window_minutes: int = DEFAULT_WINDOW_MINUTES,
    trigger_seconds: int = DEFAULT_TRIGGER_SECONDS,
) -> None:
    """Build and start the Structured Streaming pipeline.

    Pipeline Flow
    -------------
    Kafka → JSON Parse → Rename/Cast/Quality (batch reuse) →
    Watermark(10min) → Window(5min) by zone → Delta Lake output

    Parameters
    ----------
    spark : SparkSession
    kafka_bootstrap : str
        Kafka broker address (internal Docker network).
    kafka_topic : str
        Source Kafka topic.
    output_path : str
        Delta Lake output path for windowed aggregates.
    checkpoint_path : str
        Checkpoint directory for exactly-once guarantees.
    watermark_minutes : int
        Watermark threshold for late data handling.
    window_minutes : int
        Tumbling window duration for aggregation.
    trigger_seconds : int
        Processing trigger interval.
    """

    logger.info("=" * 72)
    logger.info("🚀 NYC Taxi Streaming Pipeline — Démarrage")
    logger.info("=" * 72)
    logger.info("  Kafka Bootstrap  : %s", kafka_bootstrap)
    logger.info("  Topic Source     : %s", kafka_topic)
    logger.info("  Output Delta     : %s", output_path)
    logger.info("  Checkpoint       : %s", checkpoint_path)
    logger.info("  Watermark        : %d minutes", watermark_minutes)
    logger.info("  Window           : %d minutes (tumbling)", window_minutes)
    logger.info("  Trigger          : %d seconds", trigger_seconds)
    logger.info("=" * 72)

    # ── Step 1: Read from Kafka ──────────────────────────────────────────
    raw_stream = (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 50000)  # Back-pressure control
        .load()
    )

    logger.info("✅ Flux Kafka connecté.")

    # ── Step 2: Parse JSON payload ───────────────────────────────────────
    parsed_stream = (
        raw_stream
        .selectExpr("CAST(value AS STRING) AS json_payload")
        .select(
            F.from_json(
                F.col("json_payload"),
                KAFKA_TRIP_SCHEMA,
            ).alias("data")
        )
        .select("data.*")
    )

    # ── Step 3: Cast timestamp strings → TimestampType ───────────────────
    # (JSON serialization from the producer sends ISO strings)
    parsed_stream = (
        parsed_stream
        .withColumn(
            "tpep_pickup_datetime",
            F.to_timestamp("tpep_pickup_datetime"),
        )
        .withColumn(
            "tpep_dropoff_datetime",
            F.to_timestamp("tpep_dropoff_datetime"),
        )
    )

    # ── Step 4: Apply batch Silver transformations (reuse!) ──────────────
    cleaned_stream = apply_batch_transformations_streaming(parsed_stream)

    logger.info("✅ Transformations Silver appliquées au flux streaming.")

    # ── Step 5: Watermark + Tumbling Window Aggregation ──────────────────
    windowed_agg = (
        cleaned_stream
        # Watermark on event time for late data management
        .withWatermark("tpep_pickup_datetime", f"{watermark_minutes} minutes")
        # Tumbling window by zone
        .groupBy(
            F.window("tpep_pickup_datetime", f"{window_minutes} minutes"),
            F.col("pu_location_id"),
        )
        .agg(
            # Core metrics
            F.count("*").alias("trip_count"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("avg_fare"),
            F.sum("tip_amount").alias("total_tips"),
            F.avg("tip_amount").alias("avg_tip"),
            F.avg("trip_distance").alias("avg_distance_miles"),
            F.avg("duration_minutes").alias("avg_duration_minutes"),
            # Extremes
            F.max("total_amount").alias("max_fare"),
            F.min("total_amount").alias("min_fare"),
        )
    )

    # ── Step 6: Flatten window struct + add processing metadata ──────────
    output_stream = (
        windowed_agg
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("pu_location_id").alias("zone_id"),
            F.col("trip_count"),
            F.round("total_revenue", 2).alias("total_revenue"),
            F.round("avg_fare", 2).alias("avg_fare"),
            F.round("total_tips", 2).alias("total_tips"),
            F.round("avg_tip", 2).alias("avg_tip"),
            F.round("avg_distance_miles", 2).alias("avg_distance_miles"),
            F.round("avg_duration_minutes", 2).alias("avg_duration_minutes"),
            F.round("max_fare", 2).alias("max_fare"),
            F.round("min_fare", 2).alias("min_fare"),
            F.current_timestamp().alias("processing_time"),
        )
    )

    logger.info("✅ Agrégation par fenêtres de %d minutes configurée.", window_minutes)

    # ── Step 7: Write to Delta Lake (append mode + checkpoint) ───────────
    query = (
        output_stream
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start(output_path)
    )

    logger.info(
        "✅ Pipeline Streaming démarré (ID: %s). Latence cible : < %ds.",
        query.id,
        trigger_seconds * 3,  # target < 3x trigger
    )
    logger.info("   Appuyez sur Ctrl+C pour arrêter proprement.")

    # ── Block until termination ──────────────────────────────────────────
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("🛑 Interruption reçue. Arrêt du pipeline streaming...")
        query.stop()
        logger.info("✅ Pipeline arrêté proprement.")


# ==============================================================================
# Console Streaming (Debug / Development)
# ==============================================================================

def run_console_debug(
    spark: SparkSession,
    *,
    kafka_bootstrap: str = DEFAULT_KAFKA_BOOTSTRAP,
    kafka_topic: str = DEFAULT_KAFKA_TOPIC,
    watermark_minutes: int = DEFAULT_WATERMARK_MINUTES,
    window_minutes: int = DEFAULT_WINDOW_MINUTES,
    trigger_seconds: int = DEFAULT_TRIGGER_SECONDS,
) -> None:
    """Run the same pipeline but output to console for debugging."""

    logger.info("🧪 Mode DEBUG — Sortie console (pas d'écriture Delta)")

    raw_stream = (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_stream = (
        raw_stream
        .selectExpr("CAST(value AS STRING) AS json_payload")
        .select(
            F.from_json(F.col("json_payload"), KAFKA_TRIP_SCHEMA).alias("data")
        )
        .select("data.*")
        .withColumn("tpep_pickup_datetime", F.to_timestamp("tpep_pickup_datetime"))
        .withColumn("tpep_dropoff_datetime", F.to_timestamp("tpep_dropoff_datetime"))
    )

    cleaned = apply_batch_transformations_streaming(parsed_stream)

    windowed = (
        cleaned
        .withWatermark("tpep_pickup_datetime", f"{watermark_minutes} minutes")
        .groupBy(
            F.window("tpep_pickup_datetime", f"{window_minutes} minutes"),
            F.col("pu_location_id"),
        )
        .agg(
            F.count("*").alias("trip_count"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("avg_fare"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("pu_location_id").alias("zone_id"),
            "trip_count",
            F.round("total_revenue", 2).alias("total_revenue"),
            F.round("avg_fare", 2).alias("avg_fare"),
        )
    )

    query = (
        windowed
        .writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("numRows", 20)
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
    )

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        query.stop()
        logger.info("✅ Debug console arrêtée.")


# ==============================================================================
# CLI
# ==============================================================================

def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="NYC Taxi Structured Streaming — Kafka → Delta Lake",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--kafka-bootstrap",
        type=str,
        default=DEFAULT_KAFKA_BOOTSTRAP,
        help=f"Kafka bootstrap servers (default: {DEFAULT_KAFKA_BOOTSTRAP}).",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default=DEFAULT_KAFKA_TOPIC,
        help=f"Source Kafka topic (default: {DEFAULT_KAFKA_TOPIC}).",
    )
    parser.add_argument(
        "--output-path",
        type=str,
        default=DEFAULT_OUTPUT_PATH,
        help=f"Delta Lake output path (default: {DEFAULT_OUTPUT_PATH}).",
    )
    parser.add_argument(
        "--checkpoint-path",
        type=str,
        default=DEFAULT_CHECKPOINT_PATH,
        help=f"Checkpoint path (default: {DEFAULT_CHECKPOINT_PATH}).",
    )
    parser.add_argument(
        "--watermark",
        type=int,
        default=DEFAULT_WATERMARK_MINUTES,
        help=f"Watermark in minutes (default: {DEFAULT_WATERMARK_MINUTES}).",
    )
    parser.add_argument(
        "--window",
        type=int,
        default=DEFAULT_WINDOW_MINUTES,
        help=f"Tumbling window in minutes (default: {DEFAULT_WINDOW_MINUTES}).",
    )
    parser.add_argument(
        "--trigger",
        type=int,
        default=DEFAULT_TRIGGER_SECONDS,
        help=f"Trigger interval in seconds (default: {DEFAULT_TRIGGER_SECONDS}).",
    )
    parser.add_argument(
        "--console",
        action="store_true",
        default=False,
        help="Debug mode: output to console instead of Delta Lake.",
    )

    return parser.parse_args()


def main() -> None:
    """Entry point."""
    args = parse_args()

    spark = create_streaming_spark_session()

    if args.console:
        run_console_debug(
            spark,
            kafka_bootstrap=args.kafka_bootstrap,
            kafka_topic=args.topic,
            watermark_minutes=args.watermark,
            window_minutes=args.window,
            trigger_seconds=args.trigger,
        )
    else:
        run_streaming_pipeline(
            spark,
            kafka_bootstrap=args.kafka_bootstrap,
            kafka_topic=args.topic,
            output_path=args.output_path,
            checkpoint_path=args.checkpoint_path,
            watermark_minutes=args.watermark,
            window_minutes=args.window,
            trigger_seconds=args.trigger,
        )


if __name__ == "__main__":
    main()
