---
description: Verifie la fraicheur des sources (dbt source freshness)
allowed-tools: Bash(set -a && . ./.env && set +a && ./dbt_venv/bin/dbt source freshness:*)
---

Lance le check de fraicheur des sources :

```bash
set -a && . ./.env && set +a && ./dbt_venv/bin/dbt source freshness
```

Resume les sources en `warn`/`error` (avec leur age vs seuil), en t'appuyant sur le niveau (Critique / Standard…) et les seuils de chaque source dans `docs/freshness.md`. Ne signale pas comme anormal ce qui y est assumé (ex. source en pause, warn seul).
