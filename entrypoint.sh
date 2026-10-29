#!/bin/bash
# =============================================================================
# dbt Cloud Run Job entrypoint
#
# Required env vars (set in Cloud Run Job):
#   DBT_BIGQUERY_PROJECT        — GCP project
#   DBT_BIGQUERY_DATASET_PROD   — BigQuery dataset (prod)
#   DBT_BIGQUERY_KEYFILE        — path to GCP keyfile (e.g. /secrets/gcp-key.json)
#   DBT_TARGET                  — dbt target (default: prod)
#   DBT_ARTIFACTS_URI           — gs://… où publish_run.py historise chaque
#                                 exécution (absente : rien n'est déposé)
#
# Injected by Cloud Workflow at runtime:
#   DBT_COMMAND                 — dbt command to run: "build" (default) or "snapshot"
#   DBT_SOURCE_SELECTOR         — e.g. "source:yuman_api" (skipped if empty)
#   DBT_TAG_SELECTOR            — build node selector, e.g. "tag:yuman" or
#                                 "source:yuman_api+" (source + all descendants).
#                                 Snapshots are always excluded from build — they
#                                 run via DBT_COMMAND=snapshot only.
#   DBT_SELECTOR_NAME           — named selector from selectors.yml. Takes
#                                 precedence over DBT_TAG_SELECTOR (mutually
#                                 exclusive: --selector instead of --select).
#   DBT_FULL_REFRESH            — "true" adds --full-refresh (manual runs only).
# =============================================================================

set -euo pipefail

STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
FRESHNESS_EXIT=""

# À la sortie du job, même en échec : historise l'exécution et écrit une ligne
# ERROR par échec, que l'alerte cloud_run_job_failed remonte (publish_run.py).
# Le code retour de dbt est conservé, qu'il y ait publication ou non.
publish_run() {
  local exit_code=$?
  python3 publish_run.py --exit-code "${exit_code}" --freshness-exit-code "${FRESHNESS_EXIT}" \
    --started-at "${STARTED_AT}" \
    || echo '{"severity": "WARNING", "message": "[artefacts] publish_run.py en échec : exécution non historisée"}'
  exit "${exit_code}"
}
trap publish_run EXIT

if [ -n "${DBT_SOURCE_SELECTOR:-}" ]; then
  echo "[dbt] Running source freshness: ${DBT_SOURCE_SELECTOR}"
  # Code ≠ 0 = seuil error_after franchi ou requête en échec : seul détecteur d'un
  # jour perdu sur les sources non rétroactives (docs/freshness.md). Non bloquant :
  # la donnée déjà chargée doit continuer à se transformer ; publish_run le signale.
  FRESHNESS_EXIT=0
  dbt source freshness --select "${DBT_SOURCE_SELECTOR}" || FRESHNESS_EXIT=$?
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
  # --indirect-selection=cautious (mode sélecteur = build partiel) : un test ne
  # tourne que si TOUS ses parents sont sélectionnés, pour qu'un relationships
  # franchissant le sous-graphe ne casse pas un build partiel.
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
