"""
tests.unit.test_streaming
~~~~~~~~~~~~~~~~~~~~~~~~~
Unit tests for the Streaming extension:
- Simulator / Producer logic (ingestion/stream_producer.py)
- Spark Structured Streaming transformations & schemas (spark_jobs/stream_trips.py)
"""

from __future__ import annotations

import json
from datetime import datetime
import pandas as pd
import pytest

from ingestion.stream_producer import (
    prepare_dataframe,
    row_to_json,
    DEFAULT_SPEED_FACTOR,
)
from spark_jobs.stream_trips import (
    KAFKA_TRIP_SCHEMA,
    _apply_transformations_inline,
)


class TestStreamProducer:
    """Test suite for ingestion.stream_producer logic."""

    def test_prepare_dataframe_sorting(self):
        """Verify rows are sorted chronologically by pickup_datetime."""
        data = {
            "tpep_pickup_datetime": [
                "2024-01-01 12:30:00",
                "2024-01-01 10:00:00",
                "2024-01-01 14:15:00",
            ],
            "PULocationID": [132, 161, 230],
            "fare_amount": [50.0, 15.0, 22.0],
        }
        df = pd.DataFrame(data)
        prepared = prepare_dataframe(df)

        assert len(prepared) == 3
        # Should be sorted: 10:00:00, 12:30:00, 14:15:00
        assert prepared.iloc[0]["PULocationID"] == 161
        assert prepared.iloc[1]["PULocationID"] == 132
        assert prepared.iloc[2]["PULocationID"] == 230

    def test_prepare_dataframe_max_records(self):
        """Verify limiting to max_records works."""
        data = {
            "tpep_pickup_datetime": [f"2024-01-01 10:{i:02d}:00" for i in range(10)],
            "PULocationID": list(range(10)),
        }
        df = pd.DataFrame(data)
        prepared = prepare_dataframe(df, max_records=4)
        assert len(prepared) == 4

    def test_row_to_json_serialization(self):
        """Verify pandas row is safely converted to JSON-serializable types."""
        row = pd.Series({
            "VendorID": 2,
            "tpep_pickup_datetime": pd.Timestamp("2024-01-01 10:00:00"),
            "fare_amount": 25.50,
            "missing_col": None,
        })
        record = row_to_json(row)

        # Must serialize cleanly with stdlib json
        serialized = json.dumps(record)
        deserialized = json.loads(serialized)

        assert deserialized["VendorID"] == 2
        assert "2024-01-01T10:00:00" in deserialized["tpep_pickup_datetime"]
        assert deserialized["fare_amount"] == 25.50
        assert deserialized["missing_col"] is None


class TestStreamTripsSchema:
    """Test suite for spark_jobs.stream_trips schema & transformations."""

    def test_kafka_trip_schema_fields(self):
        """Verify Kafka JSON schema contains all required TLC Yellow Taxi fields."""
        field_names = [f.name for f in KAFKA_TRIP_SCHEMA.fields]
        required_fields = [
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "PULocationID",
            "DOLocationID",
            "payment_type",
            "fare_amount",
            "tip_amount",
            "total_amount",
        ]
        for field in required_fields:
            assert field in field_names, f"Missing required field: {field}"
