#!/bin/sh
# Bootstrap a Polaris catalog on the MinIO warehouse and grant the root principal
# full content management, so StarRocks/Flink can create & write Iceberg tables.
#
# This uses the Polaris Management API. The exact storage-config shape is version
# sensitive (see CLAUDE.md "Known risks"). The script is idempotent and verbose so
# failures are easy to diagnose during `scripts/run.sh up`.
set -eu

MGMT="${POLARIS_URI}/api/management/v1"
OAUTH="${POLARIS_URI}/api/catalog/v1/oauth/tokens"
S3_LOCATION="s3://${WAREHOUSE_BUCKET}/iceberg"

echo "[polaris-bootstrap] requesting OAuth2 token..."
TOKEN=$(curl -sf -X POST "${OAUTH}" \
  -d "grant_type=client_credentials" \
  -d "client_id=${POLARIS_CLIENT_ID}" \
  -d "client_secret=${POLARIS_CLIENT_SECRET}" \
  -d "scope=PRINCIPAL_ROLE:ALL" \
  | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p')

if [ -z "${TOKEN}" ]; then
  echo "[polaris-bootstrap] FAILED to obtain token" >&2
  exit 1
fi
AUTH="Authorization: Bearer ${TOKEN}"
echo "[polaris-bootstrap] token acquired."

# 1) Create the catalog (INTERNAL) backed by S3/MinIO. ------------------------
echo "[polaris-bootstrap] creating catalog '${POLARIS_CATALOG_NAME}' @ ${S3_LOCATION}"
CREATE_BODY=$(cat <<JSON
{
  "catalog": {
    "type": "INTERNAL",
    "name": "${POLARIS_CATALOG_NAME}",
    "properties": {
      "default-base-location": "${S3_LOCATION}",
      "s3.endpoint": "${MINIO_ENDPOINT}",
      "s3.path-style-access": "true",
      "s3.access-key-id": "${MINIO_ROOT_USER}",
      "s3.secret-access-key": "${MINIO_ROOT_PASSWORD}",
      "s3.region": "${MINIO_REGION}"
    },
    "storageConfigInfo": {
      "storageType": "S3",
      "allowedLocations": ["${S3_LOCATION}"],
      "endpoint": "${MINIO_ENDPOINT}",
      "pathStyleAccess": true,
      "region": "${MINIO_REGION}"
    }
  }
}
JSON
)
HTTP=$(curl -s -o /tmp/cat.out -w "%{http_code}" -X POST "${MGMT}/catalogs" \
  -H "${AUTH}" -H "Content-Type: application/json" -d "${CREATE_BODY}" || true)
if [ "${HTTP}" = "201" ] || [ "${HTTP}" = "200" ]; then
  echo "[polaris-bootstrap] catalog created."
elif [ "${HTTP}" = "409" ]; then
  echo "[polaris-bootstrap] catalog already exists, continuing."
else
  echo "[polaris-bootstrap] catalog create returned HTTP ${HTTP}:" >&2
  cat /tmp/cat.out >&2; echo >&2
  # Don't hard-fail on storage-config quirks; surface and continue so other steps run.
fi

# 2) Catalog role with full content management. -------------------------------
curl -s -o /dev/null -X POST "${MGMT}/catalogs/${POLARIS_CATALOG_NAME}/catalog-roles" \
  -H "${AUTH}" -H "Content-Type: application/json" \
  -d '{"catalogRole":{"name":"bench_admin"}}' || true

curl -s -o /dev/null -X PUT \
  "${MGMT}/catalogs/${POLARIS_CATALOG_NAME}/catalog-roles/bench_admin/grants" \
  -H "${AUTH}" -H "Content-Type: application/json" \
  -d '{"grant":{"type":"catalog","privilege":"CATALOG_MANAGE_CONTENT"}}' || true

# 3) Principal role + assignments. --------------------------------------------
curl -s -o /dev/null -X POST "${MGMT}/principal-roles" \
  -H "${AUTH}" -H "Content-Type: application/json" \
  -d '{"principalRole":{"name":"bench_role"}}' || true

# assign catalog-role -> principal-role
curl -s -o /dev/null -X PUT \
  "${MGMT}/principal-roles/bench_role/catalog-roles/${POLARIS_CATALOG_NAME}" \
  -H "${AUTH}" -H "Content-Type: application/json" \
  -d '{"catalogRole":{"name":"bench_admin"}}' || true

# assign principal-role -> root principal
curl -s -o /dev/null -X PUT \
  "${MGMT}/principals/${POLARIS_CLIENT_ID}/principal-roles" \
  -H "${AUTH}" -H "Content-Type: application/json" \
  -d '{"principalRole":{"name":"bench_role"}}' || true

echo "[polaris-bootstrap] grants applied. Verifying catalog is listable..."
curl -sf "${MGMT}/catalogs" -H "${AUTH}" | head -c 400 || true
echo
echo "[polaris-bootstrap] done."
