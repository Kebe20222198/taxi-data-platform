"""
spark_jobs.common.spark_factory
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Factory pour créer une SparkSession pré-configurée avec :
  • Delta Lake (extensions SQL + catalog)
  • Connecteur S3A → MinIO (path-style access)
  • Tuning mémoire adapté à 16 Go RAM / 4 cœurs
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def create_spark_session(
    app_name: str = "nyc-taxi-pipeline",
    *,
    master: str | None = None,
    enable_delta: bool = True,
    enable_s3a: bool = True,
    extra_config: dict[str, str] | None = None,
) -> SparkSession:
    """Build a SparkSession with Delta Lake + S3A preconfigured.

    Parameters
    ----------
    app_name : str
        Nom de l'application Spark.
    master : str | None
        URL du master Spark.  ``None`` → utilise la valeur par défaut
        (``local[*]`` en standalone, sinon le master du cluster).
    enable_delta : bool
        Active les extensions Delta Lake.
    enable_s3a : bool
        Configure les connecteurs S3A pour pointer sur MinIO.
    extra_config : dict | None
        Clés/valeurs Spark supplémentaires.

    Returns
    -------
    SparkSession
    """
    builder = SparkSession.builder.appName(app_name)

    if master:
        builder = builder.master(master)

    # ── Delta Lake ───────────────────────────────────────────────────────
    if enable_delta:
        builder = (
            builder
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )

    # ── S3A / MinIO ──────────────────────────────────────────────────────
    if enable_s3a:
        endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
        access_key = os.getenv(
            "AWS_ACCESS_KEY_ID", os.getenv("MINIO_ROOT_USER", "minioadmin")
        )
        secret_key = os.getenv(
            "AWS_SECRET_ACCESS_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
        )
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config("spark.hadoop.fs.s3a.access.key", access_key)
            .config("spark.hadoop.fs.s3a.secret.key", secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config(
                "spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem",
            )
            # Prevent S3A from trying to find credentials via other providers
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
        )

    # ── Performance tuning (16 GB / 4 cores) ────────────────────────────
    builder = (
        builder
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.parquet.mergeSchema", "false")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
    )

    # ── Extra config ─────────────────────────────────────────────────────
    if extra_config:
        for key, value in extra_config.items():
            builder = builder.config(key, value)

    return builder.getOrCreate()
