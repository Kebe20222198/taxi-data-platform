"""
spark_jobs.common.constants
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Constantes métier et techniques pour le pipeline NYC Yellow Taxi.
"""

from __future__ import annotations

# ==============================================================================
# Airport Zone IDs (taxi_zone_lookup.csv)
# ==============================================================================
AIRPORT_ZONES: dict[int, str] = {
    1: "Newark (EWR)",
    132: "JFK Airport",
    138: "LaGuardia Airport",
}
AIRPORT_ZONE_IDS: set[int] = set(AIRPORT_ZONES.keys())

# ==============================================================================
# Quality-rule thresholds
# ==============================================================================
MAX_TRIP_DURATION_HOURS: float = 6.0
MAX_TRIP_DURATION_SECONDS: int = int(MAX_TRIP_DURATION_HOURS * 3600)  # 21 600 s
MAX_TRIP_DISTANCE_MILES: float = 100.0
VALID_ZONE_ID_MIN: int = 1
VALID_ZONE_ID_MAX: int = 265
VALID_ZONE_IDS: set[int] = set(range(VALID_ZONE_ID_MIN, VALID_ZONE_ID_MAX + 1))

# ==============================================================================
# Column rename map  (raw Parquet → snake_case)
# ==============================================================================
COLUMN_RENAME_MAP: dict[str, str] = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "tpep_pickup_datetime",
    "tpep_dropoff_datetime": "tpep_dropoff_datetime",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "RatecodeID": "ratecode_id",
    "store_and_fwd_flag": "store_and_fwd_flag",
    "PULocationID": "pu_location_id",
    "DOLocationID": "do_location_id",
    "payment_type": "payment_type",
    "fare_amount": "fare_amount",
    "extra": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "total_amount": "total_amount",
    "congestion_surcharge": "congestion_surcharge",
    "Airport_fee": "airport_fee",
}

# ==============================================================================
# Deduplication — columns used to build the SHA-256 trip fingerprint
# ==============================================================================
DEDUP_KEY_COLUMNS: list[str] = [
    "vendor_id",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "pu_location_id",
    "do_location_id",
    "trip_distance",
    "fare_amount",
]

# ==============================================================================
# MinIO / S3A paths
# ==============================================================================
BRONZE_BUCKET = "bronze"
SILVER_BUCKET = "silver"
GOLD_BUCKET = "gold"
QUARANTINE_BUCKET = "quarantine"

BRONZE_YELLOW_PATH = "s3a://bronze/yellow"
SILVER_YELLOW_PATH = "s3a://silver/yellow"
GOLD_METRICS_PATH = "s3a://gold/zone_hourly_metrics"
QUARANTINE_YELLOW_PATH = "s3a://quarantine/yellow"
BRONZE_REFERENCE_PATH = "s3a://bronze/reference"
