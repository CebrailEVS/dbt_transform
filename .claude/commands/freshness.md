---
description: Verifie la fraicheur des sources (dbt source freshness)
allowed-tools: Bash(./dbt_venv/bin/dbt source freshness:*)
---

Lance le check de fraicheur des sources :

```bash
./dbt_venv/bin/dbt source freshness
```

Resume les sources en `warn`/`error` (avec leur age vs seuil), en t'appuyant sur les tiers de `docs/freshness.md` (Critique / Standard / Relaxe). Ne signale pas comme anormal ce qui rentre dans le tier attendu.
