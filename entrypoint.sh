#!/bin/bash
# =============================================================================
# dbt Cloud Run Job entrypoint
#
# Required env vars (set in Cloud Run Job):
#   DBT_BIGQUERY_PROJECT        — GCP project
#   DBT_BIGQUERY_DATASET_PROD   — BigQuery dataset (prod)
#   DBT_BIGQUERY_KEYFILE        — path to GCP keyfile (e.g. /secrets/gcp-key.json)
#   DBT_TARGET                  — dbt target (default: prod)
#
# Injected by Cloud Workflow at runtime:
#   DBT_SOURCE_SELECTOR         — e.g. "source:yuman_api"
#   DBT_TAG_SELECTOR            — e.g. "tag:yuman"
# =============================================================================

set -euo pipefail

echo "[dbt] Installing packages..."
dbt deps

echo "[dbt] Running source freshness: ${DBT_SOURCE_SELECTOR}"
dbt source freshness --select "${DBT_SOURCE_SELECTOR}" || echo "[WARN] Source freshness had warnings, continuing..."

echo "[dbt] Building models: ${DBT_TAG_SELECTOR}"
dbt build --select "${DBT_TAG_SELECTOR}" --target "${DBT_TARGET:-prod}"

echo "[dbt] Done."
