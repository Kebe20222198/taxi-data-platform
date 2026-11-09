"""
ingestion.stream_producer
~~~~~~~~~~~~~~~~~~~~~~~~~
Simulateur / Producteur Kafka temps réel pour le pipeline NYC Yellow Taxi.

Lit un fichier Parquet de trajets depuis MinIO (couche Bronze), les trie
par horodatage de départ (tpep_pickup_datetime), et les publie en JSON
dans le topic Kafka ``taxi_trips`` en **temps accéléré** paramétrable.

Principe d'accélération
-----------------------
Par défaut, 1 heure de données réelles est rejouée en 1 minute (facteur 60x).
Le producteur calcule le delta temporel entre deux événements consécutifs et
applique ``sleep(delta_reel / speed_factor)`` pour reproduire fidèlement
la cadence de publication tout en accélérant le temps.

Usage
-----
.. code-block:: bash

    # Replay janvier 2024 à vitesse 60x (1h de données → 1min de replay)
    python ingestion/stream_producer.py --month 2024-01

    # Replay plus rapide (120x) avec un batch-size de 50
    python ingestion/stream_producer.py --month 2024-01 --speed 120 --batch-size 50

    # Lecture locale directe (sans MinIO)
    python ingestion/stream_producer.py --parquet-path data/yellow_tripdata_2024-01.parquet

    # Replay infini en boucle
    python ingestion/stream_producer.py --month 2024-01 --loop
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import os
import sys
import time
from datetime import datetime
from typing import Iterator

import pandas as pd

# ==============================================================================
# Configuration
# ==============================================================================

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "taxi_trips")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT_HOST", os.getenv("MINIO_ENDPOINT", "http://localhost:9000"))
MINIO_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", os.getenv("MINIO_ROOT_USER", "minioadmin"))
MINIO_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))
BRONZE_BUCKET = "bronze"

DEFAULT_SPEED_FACTOR = 60  # 1h de données réelles → 1 min de replay
DEFAULT_BATCH_SIZE = 20    # Messages publiés par micro-batch
DEFAULT_MAX_RECORDS = 0    # 0 = pas de limite

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("stream_producer")


# ==============================================================================
# MinIO / S3 Reader
# ==============================================================================

def read_parquet_from_minio(month: str) -> pd.DataFrame:
    """Read a Parquet file from MinIO Bronze bucket.

    Parameters
    ----------
    month : str
        Format ``YYYY-MM`` (e.g. ``2024-01``).

    Returns
    -------
    pd.DataFrame
    """
    try:
        from minio import Minio
    except ImportError:
        logger.error("Le package 'minio' n'est pas installé. pip install minio")
        sys.exit(1)

    client = Minio(
        MINIO_ENDPOINT.replace("http://", "").replace("https://", ""),
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_ENDPOINT.startswith("https"),
    )

    year, mo = month.split("-")
    object_key = f"yellow/year={year}/month={int(mo):02d}/yellow_tripdata_{month}.parquet"

    logger.info("📥 Lecture de s3://%s/%s ...", BRONZE_BUCKET, object_key)

    response = client.get_object(BRONZE_BUCKET, object_key)
    data = response.read()
    response.close()
    response.release_conn()

    df = pd.read_parquet(io.BytesIO(data))
    logger.info("✅ %s lignes chargées depuis MinIO.", f"{len(df):,}")
    return df


def read_parquet_local(path: str) -> pd.DataFrame:
    """Read a Parquet file from local filesystem."""
    logger.info("📥 Lecture locale de %s ...", path)
    df = pd.read_parquet(path)
    logger.info("✅ %s lignes chargées.", f"{len(df):,}")
    return df


# ==============================================================================
# Kafka Producer
# ==============================================================================

def create_kafka_producer():
    """Create and return a KafkaProducer with JSON serialization."""
    try:
        from kafka import KafkaProducer
    except ImportError:
        logger.error(
            "Le package 'kafka-python-ng' n'est pas installé.\n"
            "  pip install kafka-python-ng"
        )
        sys.exit(1)

    logger.info("🔌 Connexion au broker Kafka : %s", KAFKA_BOOTSTRAP)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        # Tuning — garantir la livraison et limiter la RAM
        acks="all",
        retries=3,
        batch_size=16384,        # 16 KB batches
        linger_ms=10,            # Attendre 10ms pour grouper les messages
        buffer_memory=33554432,  # 32 MB buffer max
        compression_type="snappy",
        max_request_size=1048576,
    )

    logger.info("✅ Connexion Kafka établie (topic: %s).", KAFKA_TOPIC)
    return producer


# ==============================================================================
# DataFrame Preparation
# ==============================================================================

# Colonnes à publier dans le message Kafka (sous-ensemble pertinent)
PUBLISH_COLUMNS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "Airport_fee",
]


def prepare_dataframe(df: pd.DataFrame, max_records: int = 0) -> pd.DataFrame:
    """Sort by pickup datetime and optionally limit rows.

    Parameters
    ----------
    df : pd.DataFrame
        Raw DataFrame from Parquet.
    max_records : int
        Maximum number of records to stream. 0 = all.

    Returns
    -------
    pd.DataFrame
        Sorted and cleaned DataFrame ready for streaming.
    """
    # Normaliser les noms de colonnes (certaines versions du Parquet varient)
    pickup_col = None
    for candidate in ["tpep_pickup_datetime", "pickup_datetime"]:
        if candidate in df.columns:
            pickup_col = candidate
            break

    if pickup_col is None:
        logger.error("❌ Colonne pickup_datetime introuvable. Colonnes: %s", list(df.columns))
        sys.exit(1)

    # Forcer le type datetime
    df[pickup_col] = pd.to_datetime(df[pickup_col], errors="coerce")

    # Supprimer les lignes sans horodatage
    df = df.dropna(subset=[pickup_col])

    # Trier chronologiquement
    df = df.sort_values(pickup_col).reset_index(drop=True)

    if max_records > 0:
        df = df.head(max_records)
        logger.info("🔢 Limité à %s enregistrements.", f"{max_records:,}")

    return df


def row_to_json(row: pd.Series) -> dict:
    """Convert a pandas row to a JSON-safe dictionary."""
    record = {}
    for col in row.index:
        val = row[col]
        if pd.isna(val):
            record[col] = None
        elif isinstance(val, (pd.Timestamp, datetime)):
            record[col] = val.isoformat()
        elif hasattr(val, "item"):
            # numpy scalar → python native
            record[col] = val.item()
        else:
            record[col] = val
    return record


# ==============================================================================
# Replay Engine
# ==============================================================================

def stream_events(
    df: pd.DataFrame,
    producer,
    *,
    speed_factor: float = DEFAULT_SPEED_FACTOR,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> int:
    """Stream DataFrame rows as Kafka messages with time-accelerated replay.

    Parameters
    ----------
    df : pd.DataFrame
        Sorted DataFrame to stream.
    producer : KafkaProducer
        Kafka producer instance.
    speed_factor : float
        Acceleration factor. 60 = 1h of real data in 1 minute.
    batch_size : int
        Number of messages to send per micro-batch before flushing.

    Returns
    -------
    int
        Total number of messages published.
    """
    # Identifier la colonne pickup
    pickup_col = "tpep_pickup_datetime"
    if pickup_col not in df.columns:
        pickup_col = "pickup_datetime"

    total_rows = len(df)
    published = 0
    errors = 0
    start_time = time.time()

    logger.info(
        "🚀 Démarrage du replay : %s événements, vitesse %sx (1h → %.1f min)",
        f"{total_rows:,}",
        speed_factor,
        60 / speed_factor,
    )

    # Première ligne → temps de référence
    prev_event_time = df.iloc[0][pickup_col]

    for idx, row in df.iterrows():
        current_event_time = row[pickup_col]

        # Calculer le délai inter-événement simulé
        if pd.notna(current_event_time) and pd.notna(prev_event_time):
            real_delta = (current_event_time - prev_event_time).total_seconds()
            if real_delta > 0:
                simulated_sleep = real_delta / speed_factor
                # Plafonner le sleep à 5s pour éviter les trous excessifs
                simulated_sleep = min(simulated_sleep, 5.0)
                if simulated_sleep > 0.001:
                    time.sleep(simulated_sleep)

        prev_event_time = current_event_time

        # Construire le message JSON
        record = row_to_json(row)

        # Clé de partitionnement = PULocationID (zone de départ)
        partition_key = str(row.get("PULocationID", "unknown"))

        try:
            producer.send(
                KAFKA_TOPIC,
                key=partition_key,
                value=record,
            )
            published += 1
        except Exception as e:
            errors += 1
            if errors <= 5:
                logger.warning("⚠️ Erreur d'envoi (message #%d): %s", published, e)

        # Flush par micro-batch
        if published % batch_size == 0:
            producer.flush()

        # Log de progression périodique
        if published % 5000 == 0 and published > 0:
            elapsed = time.time() - start_time
            rate = published / elapsed if elapsed > 0 else 0
            pct = (published / total_rows) * 100
            logger.info(
                "📊 Progression : %s / %s (%.1f%%) — %.0f msg/s — %s erreurs",
                f"{published:,}",
                f"{total_rows:,}",
                pct,
                rate,
                errors,
            )

    # Flush final
    producer.flush()

    elapsed = time.time() - start_time
    rate = published / elapsed if elapsed > 0 else 0

    logger.info(
        "✅ Replay terminé : %s messages publiés en %.1fs (%.0f msg/s) — %s erreurs",
        f"{published:,}",
        elapsed,
        rate,
        errors,
    )

    return published


# ==============================================================================
# CLI Entry Point
# ==============================================================================

def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="NYC Taxi Streaming Producer — Replay Parquet data into Kafka",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python ingestion/stream_producer.py --month 2024-01\n"
            "  python ingestion/stream_producer.py --month 2024-01 --speed 120 --batch-size 50\n"
            "  python ingestion/stream_producer.py --parquet-path data/sample.parquet --loop\n"
        ),
    )

    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--month",
        type=str,
        help="Mois à streamer (format YYYY-MM). Lecture depuis MinIO Bronze.",
    )
    source.add_argument(
        "--parquet-path",
        type=str,
        help="Chemin local vers un fichier Parquet à streamer.",
    )

    parser.add_argument(
        "--speed",
        type=float,
        default=DEFAULT_SPEED_FACTOR,
        help=f"Facteur d'accélération (défaut: {DEFAULT_SPEED_FACTOR}x = 1h → 1min).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Taille des micro-batchs Kafka (défaut: {DEFAULT_BATCH_SIZE}).",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=DEFAULT_MAX_RECORDS,
        help="Nombre max d'enregistrements à publier (0 = tous).",
    )
    parser.add_argument(
        "--kafka-bootstrap",
        type=str,
        default=KAFKA_BOOTSTRAP,
        help=f"Adresse du broker Kafka (défaut: {KAFKA_BOOTSTRAP}).",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default=KAFKA_TOPIC,
        help=f"Topic Kafka cible (défaut: {KAFKA_TOPIC}).",
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        default=False,
        help="Rejouer les données en boucle infinie.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Simuler sans publier dans Kafka (affiche les 5 premiers messages).",
    )

    return parser.parse_args()


def main() -> None:
    """Main entry point."""
    args = parse_args()

    # Override globals from CLI
    global KAFKA_BOOTSTRAP, KAFKA_TOPIC
    KAFKA_BOOTSTRAP = args.kafka_bootstrap
    KAFKA_TOPIC = args.topic

    # ── Load data ────────────────────────────────────────────────────────
    if args.month:
        df = read_parquet_from_minio(args.month)
    else:
        df = read_parquet_local(args.parquet_path)

    # ── Prepare ──────────────────────────────────────────────────────────
    df = prepare_dataframe(df, max_records=args.max_records)

    logger.info(
        "📋 Résumé : %s lignes | Période : %s → %s",
        f"{len(df):,}",
        df.iloc[0].get("tpep_pickup_datetime", "?"),
        df.iloc[-1].get("tpep_pickup_datetime", "?"),
    )

    # ── Dry-run mode ─────────────────────────────────────────────────────
    if args.dry_run:
        logger.info("🧪 Mode dry-run : affichage des 5 premiers messages")
        for _, row in df.head(5).iterrows():
            record = row_to_json(row)
            print(json.dumps(record, indent=2, default=str))
        logger.info("✅ Dry-run terminé. Aucun message envoyé.")
        return

    # ── Create producer ──────────────────────────────────────────────────
    producer = create_kafka_producer()

    try:
        iteration = 0
        while True:
            iteration += 1
            if args.loop:
                logger.info("🔄 Itération #%d (mode boucle)", iteration)

            stream_events(
                df,
                producer,
                speed_factor=args.speed,
                batch_size=args.batch_size,
            )

            if not args.loop:
                break

            # Pause de 5s entre les itérations
            logger.info("⏸️ Pause de 5 secondes avant la prochaine itération...")
            time.sleep(5)

    except KeyboardInterrupt:
        logger.info("\n🛑 Interruption utilisateur. Arrêt du producteur.")
    finally:
        producer.flush()
        producer.close()
        logger.info("🔌 Producteur Kafka fermé proprement.")


if __name__ == "__main__":
    main()
