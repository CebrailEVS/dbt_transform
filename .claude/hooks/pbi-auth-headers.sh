#!/usr/bin/env bash
# Helper: fetches Power BI Bearer token via client credentials (Azure AD service principal)
# Credentials: tenant/client IDs hardcoded (non-sensitive); secret fetched from GCP Secret Manager

TENANT_ID="89952e84-b730-40e0-b247-e35f466c6658"
CLIENT_ID="f8b4a457-c63b-49d8-8c9a-0f0db384f2b8"
PROJECT="evs-datastack-prod"
SECRET_NAME="pbi-client-secret"

CLIENT_SECRET=$(gcloud secrets versions access latest \
    --secret="$SECRET_NAME" \
    --project="$PROJECT" 2>/dev/null)

if [[ -z "$CLIENT_SECRET" ]]; then
    echo '{"error": "Failed to fetch pbi-client-secret from Secret Manager — check gcloud auth"}' >&2
    exit 1
fi

RESPONSE=$(curl -s -X POST \
    "https://login.microsoftonline.com/${TENANT_ID}/oauth2/v2.0/token" \
    -H "Content-Type: application/x-www-form-urlencoded" \
    -d "grant_type=client_credentials&client_id=${CLIENT_ID}&client_secret=${CLIENT_SECRET}&scope=https://analysis.windows.net/powerbi/api/.default")

TOKEN=$(echo "$RESPONSE" | grep -o '"access_token":"[^"]*"' | cut -d'"' -f4)

if [[ -z "$TOKEN" ]]; then
    echo '{"error": "Failed to obtain Power BI token from Microsoft"}' >&2
    exit 1
fi

printf '{"Authorization": "Bearer %s"}\n' "$TOKEN"
