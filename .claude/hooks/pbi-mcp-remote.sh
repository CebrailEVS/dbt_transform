#!/usr/bin/env bash
# Fetches a Power BI Bearer token, then launches mcp-remote with the token injected.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HEADERS=$("$SCRIPT_DIR/pbi-auth-headers.sh")
TOKEN=$(echo "$HEADERS" | grep -o '"Authorization": *"Bearer [^"]*"' | grep -o 'Bearer [^"]*')
if [[ -z "$TOKEN" ]]; then
    echo "Failed to get Power BI token" >&2
    exit 1
fi
exec npx mcp-remote "https://api.fabric.microsoft.com/v1/mcp/powerbi" \
    --header "Authorization:$TOKEN"
