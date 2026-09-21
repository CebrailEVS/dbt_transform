#!/usr/bin/env bash
# Hook: PostToolUse → lint .sql files in models/ after every Edit or Write
# Exit 0 = no issues; exit 2 = lint errors found (Claude sees output and fixes them)

INPUT=$(cat)
FILE_PATH=$(echo "$INPUT" | python3 -c "import sys, json; d=json.load(sys.stdin); print(d.get('tool_input', {}).get('file_path', ''))")

# Only act on .sql files inside models/
[[ "$FILE_PATH" == *.sql && "$FILE_PATH" == */models/* ]] || exit 0

# `dbt lint` (natif dbt v2) remplace SQLFluff, incompatible v2. Il lit le meme
# .sqlfluff et resout ref()/source()/macros nativement — plus de templater a
# choisir. Il ne se connecte PAS a BigQuery, mais profiles.yml resout des
# env_var() : on charge .env, sinon le lint echoue sur la resolution du profil
# et pas sur le SQL.
REPO_ROOT="$(git -C "$(dirname "$FILE_PATH")" rev-parse --show-toplevel 2>/dev/null)"
DBT_BIN="dbt"
if [[ -n "$REPO_ROOT" && -x "$REPO_ROOT/dbt_venv/bin/dbt" ]]; then
    DBT_BIN="$REPO_ROOT/dbt_venv/bin/dbt"
fi

if [[ -n "$REPO_ROOT" && -f "$REPO_ROOT/.env" ]]; then
    set -a
    # shellcheck disable=SC1091
    . "$REPO_ROOT/.env"
    set +a
fi

OUTPUT=$(cd "${REPO_ROOT:-.}" && "$DBT_BIN" lint "$FILE_PATH" 2>&1)
RC=$?

# dbt lint rend 1 sur erreur, 0 quand il n'y a que des warnings : un warning ne
# bloque donc pas l'edition, une erreur si.
if [[ $RC -ne 0 ]]; then
    echo "dbt lint failed for $FILE_PATH:" >&2
    echo "$OUTPUT" >&2
    exit 2
fi
