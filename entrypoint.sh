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
#   DBT_COMMAND                 — dbt command to run: "build" (default) or "snapshot"
#   DBT_SOURCE_SELECTOR         — e.g. "source:yuman_api" (skipped if empty)
#   DBT_TAG_SELECTOR            — build node selector, e.g. "tag:yuman" or
#                                 "source:yuman_api+" (source + all descendants).
#                                 Snapshots are always excluded from build — they
#                                 run via DBT_COMMAND=snapshot only.
#   DBT_SELECTOR_NAME           — named selector from selectors.yml (e.g.
#                                 "passage_appro_fastlane"). Takes precedence over
#                                 DBT_TAG_SELECTOR when set (mutually exclusive:
#                                 dbt build uses --selector instead of --select).
# =============================================================================

set -euo pipefail

echo "[dbt] Installing packages..."
dbt deps

if [ -n "${DBT_SOURCE_SELECTOR:-}" ]; then
  echo "[dbt] Running source freshness: ${DBT_SOURCE_SELECTOR}"
  dbt source freshness --select "${DBT_SOURCE_SELECTOR}" || echo "[WARN] Source freshness had warnings, continuing..."
fi

DBT_COMMAND="${DBT_COMMAND:-build}"

if [ "${DBT_COMMAND}" = "snapshot" ]; then
  echo "[dbt] Running snapshots (all)"
  dbt snapshot --target "${DBT_TARGET:-prod}"
else
  FULL_REFRESH_FLAG=""
  if [ "${DBT_FULL_REFRESH:-false}" = "true" ]; then
    FULL_REFRESH_FLAG="--full-refresh"
  fi
  # Selection mode: a named selector (selectors.yml) takes precedence over the
  # inline --select. They are mutually exclusive — never pass both to dbt build.
  #
  # --indirect-selection=cautious (selector mode = voie rapide / build partiel) :
  # un test ne tourne que si TOUS ses modèles parents sont dans la sélection. Les
  # tests relationships qui franchissent la frontière du sous-graphe (ex.
  # task→task_type, company→company_type) sont donc ignorés ici et restent testés
  # par le build nocturne complet — pas de flapping sur un build partiel.
  if [ -n "${DBT_SELECTOR_NAME:-}" ]; then
    SELECTION_ARGS=(--selector "${DBT_SELECTOR_NAME}" --indirect-selection cautious)
    echo "[dbt] Building via selector: ${DBT_SELECTOR_NAME} (indirect-selection=cautious)${FULL_REFRESH_FLAG:+ (full-refresh)}"
  else
    SELECTION_ARGS=(--select "${DBT_TAG_SELECTOR}")
    echo "[dbt] Building models: ${DBT_TAG_SELECTOR}${FULL_REFRESH_FLAG:+ (full-refresh)}"
  fi
  dbt build "${SELECTION_ARGS[@]}" --exclude resource_type:snapshot --target "${DBT_TARGET:-prod}" ${FULL_REFRESH_FLAG}
fi

echo "[dbt] Done."
