#!/usr/bin/env python3
"""
NYC Yellow Taxi Trip Data — Ingestion Script (Phase 1)

Downloads monthly Parquet files from the NYC TLC website, the taxi zone
reference CSV, and generates a US public holidays calendar. Uploads
everything to the MinIO-backed Data Lake (Bronze layer) with SHA-256
idempotence checks.

Usage:
    python ingest.py --month 2025-01
    python ingest.py --month 2025-03 --endpoint http://localhost:9000

Bucket layout:
    s3a://bronze/yellow/year=YYYY/month=MM/<file>.parquet
    s3a://bronze/reference/taxi_zone_lookup.csv
    s3a://bronze/reference/us_public_holidays.csv
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import holidays
import pandas as pd
try:
    import pyarrow.parquet as pq
except ImportError:
    pq = None
import requests
from minio import Minio
from minio.error import S3Error

# ==============================================================================
# Logging — structured JSON for observability
# ==============================================================================

class StructuredFormatter(logging.Formatter):
    """Emit log records as single-line JSON objects."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry: dict = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        # Merge extra fields injected by callers
        if hasattr(record, "extra_fields"):
            log_entry.update(record.extra_fields)
        return json.dumps(log_entry, default=str)


def _get_logger(name: str = "ingestion") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(StructuredFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


log = _get_logger()


# ==============================================================================
# Configuration
# ==============================================================================

NYC_TLC_BASE_URL = os.getenv(
    "NYC_TLC_BASE_URL",
    "https://d37ci6vzurychx.cloudfront.net/trip-data",
)
NYC_TLC_REFERENCE_URL = os.getenv(
    "NYC_TLC_REFERENCE_URL",
    "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv",
)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT_HOST", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", os.getenv("MINIO_ROOT_USER", "minioadmin"))
MINIO_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BRONZE_BUCKET = "bronze"

# Holiday calendar range (covers most useful historical + near-future data)
HOLIDAYS_YEAR_START = 2009
HOLIDAYS_YEAR_END = 2030


# ==============================================================================
# MinIO helpers
# ==============================================================================

def _minio_client() -> Minio:
    """Build a MinIO client from environment variables."""
    endpoint = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
    secure = MINIO_ENDPOINT.startswith("https://")
    return Minio(
        endpoint,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=secure,
    )


def _sha256(data: bytes) -> str:
    """Compute the SHA-256 hex digest of raw bytes."""
    return hashlib.sha256(data).hexdigest()


def _remote_sha256(client: Minio, bucket: str, key: str) -> str | None:
    """Read the SHA-256 stored in the object's user metadata, or None."""
    try:
        stat = client.stat_object(bucket, key)
        return (stat.metadata or {}).get("x-amz-meta-sha256")
    except S3Error as exc:
        if exc.code == "NoSuchKey":
            return None
        raise


def _upload_bytes(
    client: Minio,
    bucket: str,
    key: str,
    data: bytes,
    content_type: str = "application/octet-stream",
) -> bool:
    """Upload *data* to *bucket/key* with SHA-256 idempotence.

    Returns True if an upload was performed, False if skipped (identical hash).
    """
    local_hash = _sha256(data)
    remote_hash = _remote_sha256(client, bucket, key)

    if local_hash == remote_hash:
        log.info(
            "Skipped upload (identical SHA-256)",
            extra={"extra_fields": {"key": key, "sha256": local_hash}},
        )
        return False

    client.put_object(
        bucket,
        key,
        io.BytesIO(data),
        length=len(data),
        content_type=content_type,
        metadata={"sha256": local_hash},
    )
    log.info(
        "Uploaded object",
        extra={
            "extra_fields": {
                "key": key,
                "size_mb": round(len(data) / 1_048_576, 2),
                "sha256": local_hash,
            }
        },
    )
    return True


# ==============================================================================
# Download helpers
# ==============================================================================

def _download(url: str, description: str) -> bytes:
    """Download a URL with progress logging and return raw bytes."""
    log.info(f"Downloading {description}", extra={"extra_fields": {"url": url}})
    resp = requests.get(url, stream=True, timeout=300)
    resp.raise_for_status()

    chunks: list[bytes] = []
    total = 0
    for chunk in resp.iter_content(chunk_size=8_192):
        chunks.append(chunk)
        total += len(chunk)

    data = b"".join(chunks)
    log.info(
        f"Download complete: {description}",
        extra={"extra_fields": {"size_mb": round(total / 1_048_576, 2)}},
    )
    return data


# ==============================================================================
# Data generators
# ==============================================================================

def generate_us_holidays_csv() -> bytes:
    """Generate a CSV of US public holidays (~730 rows, 2009-2030).

    Columns: date, holiday_name, year, month, day, day_of_week
    """
    rows: list[dict] = []
    for year in range(HOLIDAYS_YEAR_START, HOLIDAYS_YEAR_END + 1):
        for date, name in sorted(holidays.US(years=year).items()):
            rows.append(
                {
                    "date": date.isoformat(),
                    "holiday_name": name,
                    "year": date.year,
                    "month": date.month,
                    "day": date.day,
                    "day_of_week": date.strftime("%A"),
                }
            )

    buf = io.StringIO()
    writer = csv.DictWriter(
        buf,
        fieldnames=["date", "holiday_name", "year", "month", "day", "day_of_week"],
    )
    writer.writeheader()
    writer.writerows(rows)
    data = buf.getvalue().encode("utf-8")
    log.info(
        "Generated US holidays calendar",
        extra={"extra_fields": {"rows": len(rows), "size_kb": round(len(data) / 1024, 1)}},
    )
    return data


# ==============================================================================
# Ingestion logic
# ==============================================================================

def ingest_trip_data(client: Minio, year: int, month: int, source: str = "web") -> dict:
    """Download or load locally, then upload one month of Yellow Taxi trip records.

    Returns a summary dict for the pipeline_runs log.
    """
    month_str = f"{year:04d}-{month:02d}"
    parquet_filename = f"yellow_tripdata_{month_str}.parquet"
    url = f"{NYC_TLC_BASE_URL}/{parquet_filename}"

    t0 = time.monotonic()
    status = "success"
    row_count = 0
    size_mb = 0.0

    try:
        if source == "local":
            candidates = [
                PROJECT_ROOT / "data" / "raw" / "yellow" / parquet_filename,
                PROJECT_ROOT / "data" / "yellow" / parquet_filename,
                Path("data/raw/yellow") / parquet_filename,
            ]
            local_path = next((p for p in candidates if p.exists()), None)
            if local_path is None:
                raise FileNotFoundError(
                    f"Fichier local introuvable: {parquet_filename} dans {[str(c) for c in candidates]}"
                )
            log.info(f"Loading local Parquet: {local_path}")
            data = local_path.read_bytes()
        else:
            data = _download(url, f"Yellow Taxi trips {month_str}")

        size_mb = round(len(data) / 1_048_576, 2)

        # Count rows via Parquet footer metadata (ultrafast, near-zero RAM)
        try:
            if pq is not None:
                pf = pq.ParquetFile(io.BytesIO(data))
                row_count = pf.metadata.num_rows
            else:
                df = pd.read_parquet(io.BytesIO(data))
                row_count = len(df)
        except Exception:
            # If parsing fails, still upload the file
            log.warning("Could not count rows, proceeding with upload")

        key = f"yellow/year={year:04d}/month={month:02d}/{parquet_filename}"
        uploaded = _upload_bytes(client, BRONZE_BUCKET, key, data, "application/octet-stream")

        if not uploaded:
            status = "skipped"

    except requests.HTTPError as exc:
        status = f"http_error_{exc.response.status_code}"
        log.error(
            f"Failed to download trip data for {month_str}",
            extra={"extra_fields": {"status_code": exc.response.status_code, "url": url}},
        )
    except Exception as exc:
        status = "error"
        log.error(
            f"Ingestion failed for {month_str}: {exc}",
            extra={"extra_fields": {"error": str(exc)}},
        )

    duration_s = round(time.monotonic() - t0, 2)

    return {
        "month": month_str,
        "row_count": row_count,
        "size_mb": size_mb,
        "duration_s": duration_s,
        "status": status,
        "source": source,
    }


def ingest_reference_data(client: Minio, source: str = "web") -> None:
    """Download or read locally, then upload reference tables (taxi zones, holidays, weather).

    Idempotent: skips upload if SHA-256 matches existing object.
    """
    # ── 1. Taxi Zone Lookup ───────────────────────────────────────────────
    local_zones = PROJECT_ROOT / "data" / "raw" / "reference" / "taxi_zone_lookup.csv"
    if source == "local" and local_zones.exists():
        log.info(f"Loading local taxi zone lookup: {local_zones}")
        taxi_zones = local_zones.read_bytes()
    else:
        try:
            taxi_zones = _download(NYC_TLC_REFERENCE_URL, "taxi_zone_lookup.csv")
        except Exception:
            if local_zones.exists():
                taxi_zones = local_zones.read_bytes()
            else:
                raise

    _upload_bytes(
        client,
        BRONZE_BUCKET,
        "reference/taxi_zone_lookup.csv",
        taxi_zones,
        "text/csv",
    )

    # ── 2. US Public Holidays ─────────────────────────────────────────────
    holidays_csv = generate_us_holidays_csv()
    _upload_bytes(
        client,
        BRONZE_BUCKET,
        "reference/us_public_holidays.csv",
        holidays_csv,
        "text/csv",
    )

    # ── 3. Open-Meteo Weather Data (NYC 2025) ─────────────────────────────
    local_weather = PROJECT_ROOT / "data" / "raw" / "reference" / "nyc_weather_2025.csv"
    if local_weather.exists():
        log.info(f"Loading weather reference data: {local_weather}")
        weather_data = local_weather.read_bytes()
        _upload_bytes(
            client,
            BRONZE_BUCKET,
            "reference/nyc_weather_2025.csv",
            weather_data,
            "text/csv",
        )
    else:
        log.warning(f"Weather reference file not found: {local_weather}")


# ==============================================================================
# Pipeline run log
# ==============================================================================

def log_pipeline_run(run_summary: dict) -> None:
    """Emit a structured log line for observability / pipeline_runs tracking."""
    log.info(
        "pipeline_run",
        extra={
            "extra_fields": {
                "pipeline": "ingestion",
                "dataset": "yellow_taxi",
                **run_summary,
                "completed_at": datetime.now(timezone.utc).isoformat(),
            }
        },
    )


# ==============================================================================
# CLI
# ==============================================================================

def parse_month(value: str) -> tuple[int, int]:
    """Parse a YYYY-MM string into (year, month)."""
    try:
        dt = datetime.strptime(value, "%Y-%m")
        return dt.year, dt.month
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid month format '{value}'. Expected YYYY-MM (e.g. 2025-01)."
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest NYC Yellow Taxi trip data into the Bronze layer (MinIO).",
    )
    parser.add_argument(
        "--month",
        type=str,
        required=True,
        help="Month to ingest in YYYY-MM format (e.g. 2025-01).",
    )
    parser.add_argument(
        "--source",
        type=str,
        choices=["local", "web"],
        default="local",
        help="Source provider: 'local' (reads from data/raw/yellow/) or 'web' (NYC TLC Cloudfront).",
    )
    parser.add_argument(
        "--endpoint",
        type=str,
        default=None,
        help="MinIO endpoint URL (overrides MINIO_ENDPOINT_HOST env var).",
    )
    parser.add_argument(
        "--skip-reference",
        action="store_true",
        help="Skip downloading reference tables (taxi zones, holidays, weather).",
    )
    args = parser.parse_args()

    year, month = parse_month(args.month)

    # Override endpoint if provided via CLI
    if args.endpoint:
        global MINIO_ENDPOINT
        MINIO_ENDPOINT = args.endpoint

    log.info(
        "Starting ingestion",
        extra={
            "extra_fields": {
                "month": args.month,
                "source": args.source,
                "endpoint": MINIO_ENDPOINT,
            }
        },
    )

    client = _minio_client()

    # Verify bucket exists
    if not client.bucket_exists(BRONZE_BUCKET):
        log.error(
            f"Bucket '{BRONZE_BUCKET}' does not exist. Run 'make up' first to initialize MinIO."
        )
        sys.exit(1)

    # ── Reference data (once) ────────────────────────────────────────────
    if not args.skip_reference:
        ingest_reference_data(client, source=args.source)

    # ── Trip data (monthly) ──────────────────────────────────────────────
    run_summary = ingest_trip_data(client, year, month, source=args.source)
    log_pipeline_run(run_summary)

    # ── Final report ─────────────────────────────────────────────────────
    if run_summary["status"] in ("success", "skipped"):
        log.info(
            "Ingestion completed",
            extra={"extra_fields": run_summary},
        )
    else:
        log.error(
            "Ingestion failed",
            extra={"extra_fields": run_summary},
        )
        sys.exit(1)


if __name__ == "__main__":
    main()

