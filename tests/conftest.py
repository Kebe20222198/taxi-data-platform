"""
Shared pytest fixtures for the NYC Taxi Data Platform test suite.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

# ── Ensure spark_jobs package is importable ──────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Session-scoped local SparkSession for unit tests.

    - No Delta Lake (not needed for pure transformation tests).
    - No S3A (tests run on local DataFrames).
    - Minimal shuffle partitions for speed.
    - UI disabled to avoid port conflicts.
    """
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("taxi-platform-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()
