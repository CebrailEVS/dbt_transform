#!/usr/bin/env bash
# Fetches a Power BI Bearer token, then launches mcp-remote with the token injected.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HEADERS=$("$SCRIPT_DIR/pbi-auth-headers.sh")
TOKEN=$(echo "$HEADERS" | grep -o '"Authorization": *"Bearer [^"]*"' | grep -o 'Bearer [^"]*')
if [[ -z "$TOKEN" ]]; then
    echo "Failed to get Power BI token" >&2
    exit 1
fi
# Le jeton passe par l'environnement : mcp-remote remplace ${PBI_AUTHORIZATION}
# lui-même. En argument, il serait lisible dans la liste des processus.
# Version épinglée : une nouvelle version s'adopte volontairement.
export PBI_AUTHORIZATION="$TOKEN"
exec npx -y mcp-remote@0.16.0 "https://api.fabric.microsoft.com/v1/mcp/powerbi" \
    --header 'Authorization:${PBI_AUTHORIZATION}'
