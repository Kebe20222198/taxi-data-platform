#!/bin/sh
# ==============================================================================
# MinIO Initialization Script
# Creates the Data Lakehouse bucket structure:
#   bronze/      — Raw data as-is from source
#   silver/      — Cleansed, conformed, deduplicated data
#   gold/        — Business-level aggregates & KPIs
#   quarantine/  — Rows that failed validation rules
# ==============================================================================
set -e

echo "════════════════════════════════════════════════════════════════"
echo "  MinIO Lakehouse Initialization"
echo "════════════════════════════════════════════════════════════════"

echo "⏳ Waiting for MinIO to become available..."
until /usr/bin/mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" > /dev/null 2>&1; do
  echo "   MinIO not ready yet, retrying in 2s..."
  sleep 2
done
echo "✅ MinIO connection established."

echo ""
echo "📦 Creating Lakehouse buckets..."
/usr/bin/mc mb --ignore-existing local/bronze
/usr/bin/mc mb --ignore-existing local/silver
/usr/bin/mc mb --ignore-existing local/gold
/usr/bin/mc mb --ignore-existing local/quarantine

echo ""
echo "📋 Bucket inventory:"
/usr/bin/mc ls local/

echo ""
echo "════════════════════════════════════════════════════════════════"
echo "  ✅ MinIO initialization completed successfully!"
echo "════════════════════════════════════════════════════════════════"
