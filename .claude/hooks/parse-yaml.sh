#!/usr/bin/env bash
# Hook: PostToolUse → run dbt parse after every .yml edit in models/
# Catches broken ref(), missing columns, and YAML schema errors immediately.
# Exit 0 = parse OK; exit 2 = parse error (Claude sees output and fixes it)

INPUT=$(cat)
FILE_PATH=$(echo "$INPUT" | python3 -c "import sys, json; d=json.load(sys.stdin); print(d.get('tool_input', {}).get('file_path', ''))")

# Only act on .yml files inside models/
[[ "$FILE_PATH" == *.yml && "$FILE_PATH" == */models/* ]] || exit 0

# Resolve project root from this hook's location (.claude/hooks/ → two levels up).
# Keeps the hook portable across machines / checkout paths.
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# Load project env vars so dbt can connect
ENV_FILE="${PROJECT_DIR}/.env"
if [[ -f "$ENV_FILE" ]]; then
    set -a
    # shellcheck source=/dev/null
    source "$ENV_FILE"
    set +a
fi

OUTPUT=$(cd "$PROJECT_DIR" && ./dbt_venv/bin/dbt parse 2>&1)
RC=$?

if [[ $RC -ne 0 ]]; then
    echo "dbt parse failed after editing $FILE_PATH:" >&2
    echo "$OUTPUT" >&2
    exit 2
fi
