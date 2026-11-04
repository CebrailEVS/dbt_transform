#!/usr/bin/env bash
# Launcher for the dbt MCP server (dbt Labs `dbt-mcp`), local dbt Core mode.
# Sources .env so dbt can resolve profiles.yml — no secrets stored in .mcp.json.
# Runs via `pipx run` on python3.12 (dbt-mcp requires >=3.12): isolated, ephemeral,
# version pinned (a new release is adopted on purpose, not on the next launch),
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
export DISABLE_DBT_CODEGEN="true"   # un staging suit docs/conventions/staging.md, pas le générateur générique
# Liste blanche : seuls les outils servis à Claude. Le reste (build, run, show…)
# passe par la CLI, où les règles de permission de settings.json s'appliquent.
export DBT_MCP_ENABLE_TOOLS="get_lineage_dev,get_node_details_dev"

exec pipx run --python /usr/bin/python3.12 --spec dbt-mcp==2.10.0 dbt-mcp
