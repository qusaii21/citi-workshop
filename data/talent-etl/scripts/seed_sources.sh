#!/usr/bin/env bash
# Seeds the two source systems of the talent pipeline from the provided
# sample data (sampleData/data-engineering-sample-performance-scenario.zip):
#
#   PostgreSQL : runs the provided schema.sql + load.sh (unchanged) inside a
#                dedicated schema, so reruns are safe and nothing lands in public.
#   S3         : uploads the provided learning_activity/ JSONL partitions to
#                s3://$SOURCE_BUCKET/learning_activity/.
#
# Idempotent: the source schema is dropped and recreated, and the S3 upload is
# a sync with --delete. Uses the workshop env vars (IS_LOCAL, POSTGRES_*); when
# local, falls back to the defaults from bin/setup-environment.sh and points
# the AWS CLI at LocalStack with test credentials (as bin/deploy-backend.sh does).
set -euo pipefail

JOB_DIR="$(cd "$(dirname "$0")/.." && pwd)"
REPO_DIR="$(cd "$JOB_DIR/../.." && pwd)"
ZIP_FILE="${SEED_ZIP:-$REPO_DIR/sampleData/data-engineering-sample-performance-scenario.zip}"
SEED_DIR="$JOB_DIR/.seed"
DATASET_DIR="$SEED_DIR/data-engineering-performance-talent-management"
TABLES=(teams competencies employees projects performance_reviews competency_assessments talent_review)

export IS_LOCAL="${IS_LOCAL:-true}"
SOURCE_SCHEMA="${POSTGRES_SOURCE_SCHEMA:-hr_source}"

if [ "$IS_LOCAL" = "true" ]; then
    export POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
    export POSTGRES_PORT="${POSTGRES_PORT:-5432}"
    export POSTGRES_NAME="${POSTGRES_NAME:-postgres}"
    export POSTGRES_USER="${POSTGRES_USER:-postgres}"
    export POSTGRES_PASS="${POSTGRES_PASS:-postgres123}"
    export AWS_ENDPOINT_URL="${AWS_ENDPOINT_URL_S3:-${AWS_ENDPOINT_URL:-http://localhost:4566}}"
    export AWS_ACCESS_KEY_ID=test
    export AWS_SECRET_ACCESS_KEY=test
    export AWS_REGION=us-east-1
    unset AWS_SESSION_TOKEN
    SOURCE_BUCKET="${SOURCE_BUCKET:-acme-talent-source}"
else
    : "${POSTGRES_HOST:?POSTGRES_HOST is required in cloud mode}"
    : "${POSTGRES_NAME:?POSTGRES_NAME is required in cloud mode}"
    : "${POSTGRES_USER:?POSTGRES_USER is required in cloud mode}"
    : "${POSTGRES_PASS:?POSTGRES_PASS is required in cloud mode}"
    : "${SOURCE_BUCKET:?SOURCE_BUCKET is required in cloud mode}"
fi

if ! [[ "$SOURCE_SCHEMA" =~ ^[a-z_][a-z0-9_]*$ ]]; then
    echo "ERROR: invalid POSTGRES_SOURCE_SCHEMA '$SOURCE_SCHEMA'" >&2
    exit 1
fi

SSLMODE=""
if [ "$IS_LOCAL" != "true" ]; then
    SSLMODE="sslmode=require"
fi
CONN="host=${POSTGRES_HOST} port=${POSTGRES_PORT:-5432} dbname=${POSTGRES_NAME} user=${POSTGRES_USER} password=${POSTGRES_PASS} ${SSLMODE}"

echo "[1/4] Extracting $(basename "$ZIP_FILE") ..."
rm -rf "$SEED_DIR"
mkdir -p "$SEED_DIR"
unzip -q "$ZIP_FILE" -d "$SEED_DIR"

echo "[2/4] Recreating PostgreSQL schema '$SOURCE_SCHEMA' and running provided load.sh ..."
psql "$CONN" -q -v ON_ERROR_STOP=1 \
    -c "DROP SCHEMA IF EXISTS ${SOURCE_SCHEMA} CASCADE" \
    -c "CREATE SCHEMA ${SOURCE_SCHEMA}"
PGOPTIONS="-c search_path=${SOURCE_SCHEMA}" bash "$DATASET_DIR/load.sh"

echo "[3/4] Reconciling PostgreSQL row counts against CSV files ..."
status=0
for table in "${TABLES[@]}"; do
    expected=$(( $(wc -l < "$DATASET_DIR/${table}.csv") - 1 ))
    actual=$(psql "$CONN" -tA -c "SELECT count(*) FROM ${SOURCE_SCHEMA}.${table}")
    if [ "$expected" = "$actual" ]; then
        printf "  OK    %-24s %6s rows\n" "$table" "$actual"
    else
        printf "  FAIL  %-24s csv=%s db=%s\n" "$table" "$expected" "$actual"
        status=1
    fi
done
[ "$status" -eq 0 ] || { echo "ERROR: row count mismatch" >&2; exit 1; }

echo "[4/4] Syncing learning_activity/ to s3://${SOURCE_BUCKET}/learning_activity/ ..."
if ! aws s3api head-bucket --bucket "$SOURCE_BUCKET" >/dev/null 2>&1; then
    aws s3 mb "s3://${SOURCE_BUCKET}"
fi
aws s3 sync --delete --only-show-errors \
    "$DATASET_DIR/learning_activity/" "s3://${SOURCE_BUCKET}/learning_activity/"
local_files=$(find "$DATASET_DIR/learning_activity" -type f | wc -l)
remote_files=$(aws s3 ls --recursive "s3://${SOURCE_BUCKET}/learning_activity/" | wc -l)
if [ "$local_files" -ne "$remote_files" ]; then
    echo "ERROR: S3 object count mismatch (local=${local_files} s3=${remote_files})" >&2
    exit 1
fi
echo "  OK    learning_activity        ${remote_files} files"

echo "Done. Sources seeded: postgres schema '${SOURCE_SCHEMA}', s3://${SOURCE_BUCKET}/learning_activity/"
