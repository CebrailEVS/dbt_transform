#!/bin/bash
# =============================================================================
# dbt Cloud Run Job entrypoint
#
# Required env vars (set in Cloud Run Job):
#   DBT_BIGQUERY_PROJECT        — GCP project
#   DBT_BIGQUERY_DATASET_PROD   — BigQuery dataset (prod)
#   DBT_BIGQUERY_KEYFILE        — path to GCP keyfile (e.g. /secrets/gcp-key.json)
#   DBT_TARGET                  — dbt target (default: prod)
#   DBT_STATE_BUCKET            — GCS bucket name for manifest state
#
# Injected by Cloud Workflow at runtime (optional):
#   DBT_SOURCE_SELECTOR         — e.g. "source:yuman_api" (skipped if not set)
#   DBT_TAG_SELECTOR            — e.g. "tag:yuman" (full build if not set)
# =============================================================================

set -euo pipefail

echo "[dbt] Installing packages..."
dbt deps

if [ -n "${DBT_SOURCE_SELECTOR:-}" ]; then
  echo "[dbt] Running source freshness: ${DBT_SOURCE_SELECTOR}"
  dbt source freshness --select "${DBT_SOURCE_SELECTOR}" || echo "[WARN] Source freshness had warnings, continuing..."
else
  echo "[dbt] No source selector set, skipping source freshness."
fi

FULL_REFRESH_FLAG=""
if [ "${DBT_FULL_REFRESH:-false}" = "true" ]; then
  FULL_REFRESH_FLAG="--full-refresh"
fi

if [ -n "${DBT_TAG_SELECTOR:-}" ]; then
  echo "[dbt] Building models: ${DBT_TAG_SELECTOR}${FULL_REFRESH_FLAG:+ (full-refresh)}"
  dbt build --select "${DBT_TAG_SELECTOR}" --target "${DBT_TARGET:-prod}" ${FULL_REFRESH_FLAG}
else
  echo "[dbt] No tag selector set, running full build (excluding snapshots)${FULL_REFRESH_FLAG:+ (full-refresh)}..."
  dbt build --target "${DBT_TARGET:-prod}" --exclude resource_type:snapshot ${FULL_REFRESH_FLAG}
fi

echo "[dbt] Uploading manifest to GCS..."
gsutil cp target/manifest.json gs://${DBT_STATE_BUCKET}/dbt-state/manifest.json

echo "[dbt] Done."
