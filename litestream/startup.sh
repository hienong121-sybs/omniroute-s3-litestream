#!/bin/sh
set -e

DB_PATH="/app/data/storage.sqlite"
REPLICA_URL="s3://${LITESTREAM_BUCKET}/storage?endpoint=${SUPABASE_PROJECT_REF}.supabase.co/storage/v1/s3"

echo "[startup] Checking for existing data at ${DB_PATH}..."

if [ ! -f "${DB_PATH}" ]; then
  echo "[startup] No local DB found. Attempting restore from Supabase S3..."

  litestream restore \
    -if-replica-exists \
    -o "${DB_PATH}" \
    "${REPLICA_URL}" && echo "[startup] Restore successful." \
  || echo "[startup] No remote backup found. Starting fresh."
else
  echo "[startup] Local DB already exists. Skipping restore."
fi

echo "[startup] Starting Litestream replication..."
exec litestream replicate