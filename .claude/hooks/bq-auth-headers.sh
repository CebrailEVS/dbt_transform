#!/usr/bin/env bash
# Helper: outputs Authorization + project headers for BigQuery MCP
# Uses the dbt service account keyfile — no interactive gcloud login needed
KEYFILE="${DBT_BIGQUERY_KEYFILE:-/opt/credentials/gcp-meltano-key.json}"
TOKEN=$(GOOGLE_APPLICATION_CREDENTIALS="$KEYFILE" gcloud auth application-default print-access-token 2>/dev/null)
if [[ -z "$TOKEN" ]]; then
    echo '{"error": "Failed to get token from service account keyfile — check KEYFILE path"}' >&2
    exit 1
fi
printf '{"Authorization": "Bearer %s", "x-goog-user-project": "evs-datastack-prod"}\n' "$TOKEN"
