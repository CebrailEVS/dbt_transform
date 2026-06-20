#!/usr/bin/env bash
# Hook: PostToolUse → lint .sql files in models/ after every Edit or Write
# Exit 0 = no issues; exit 2 = lint errors found (Claude sees output and fixes them)

INPUT=$(cat)
FILE_PATH=$(echo "$INPUT" | python3 -c "import sys, json; d=json.load(sys.stdin); print(d.get('tool_input', {}).get('file_path', ''))")

# Only act on .sql files inside models/
[[ "$FILE_PATH" == *.sql && "$FILE_PATH" == */models/* ]] || exit 0

# Use dbt templater to correctly resolve ref(), source(), and custom macros.
# Jinja templater was faster but broke on dbt-specific Jinja (ref, dbt_utils, project macros).
REPO_ROOT="$(git -C "$(dirname "$FILE_PATH")" rev-parse --show-toplevel 2>/dev/null)"
SQLFLUFF_BIN="sqlfluff"
if [[ -n "$REPO_ROOT" && -x "$REPO_ROOT/dbt_venv/bin/sqlfluff" ]]; then
    SQLFLUFF_BIN="$REPO_ROOT/dbt_venv/bin/sqlfluff"
fi
OUTPUT=$("$SQLFLUFF_BIN" lint "$FILE_PATH" 2>&1)
RC=$?

if [[ $RC -ne 0 ]]; then
    echo "SQLFluff lint failed for $FILE_PATH:" >&2
    echo "$OUTPUT" >&2
    exit 2
fi
