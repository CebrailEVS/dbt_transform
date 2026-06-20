#!/usr/bin/env bash
# Launcher for the dbt MCP server (dbt Labs `dbt-mcp`), local dbt Core mode.
# Sources .env so dbt can resolve profiles.yml — no secrets stored in .mcp.json.
# Runs via `pipx run` on python3.12 (dbt-mcp requires >=3.12): isolated, ephemeral,
# does NOT touch dbt_venv (python 3.11) — dbt itself is invoked via DBT_PATH.
set -euo pipefail

# Resolve project root from this launcher's location (.claude/hooks/ → two levels up).
# Keeps the launcher portable across machines / checkout paths.
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# Load dbt env vars (DBT_BIGQUERY_PROJECT, DBT_BIGQUERY_KEYFILE, etc.) for profile resolution.
set -a
# shellcheck disable=SC1091
source "${PROJECT_DIR}/.env"
set +a

# MCP server config — local Core only; everything requiring dbt Cloud is disabled.
export DBT_PROJECT_DIR="${PROJECT_DIR}"
export DBT_PATH="${PROJECT_DIR}/dbt_venv/bin/dbt"
export DBT_PROFILES_DIR="${PROJECT_DIR}"
export DISABLE_SEMANTIC_LAYER="true"
export DISABLE_DISCOVERY="true"
export DISABLE_ADMIN_API="true"
export DISABLE_SQL="true"
export DISABLE_LSP="true"
export DISABLE_DBT_CODEGEN="false"

exec pipx run --python /usr/bin/python3.12 --spec dbt-mcp dbt-mcp
