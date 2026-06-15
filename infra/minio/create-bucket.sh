#!/bin/sh
# Create the warehouse bucket used by Iceberg/Paimon. Idempotent.
set -eu

echo "[minio-init] configuring mc alias -> ${MINIO_ENDPOINT}"
mc alias set local "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"

echo "[minio-init] ensuring bucket: ${WAREHOUSE_BUCKET}"
mc mb --ignore-existing "local/${WAREHOUSE_BUCKET}"

# Optional: separate prefixes per format keep things tidy in the console.
mc mb --ignore-existing "local/${WAREHOUSE_BUCKET}/iceberg"
mc mb --ignore-existing "local/${WAREHOUSE_BUCKET}/paimon"

echo "[minio-init] done. buckets:"
mc ls local
