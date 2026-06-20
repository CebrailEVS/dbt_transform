---
description: Build (run + test, ordre DAG) tous les modeles d'une source via son tag
argument-hint: <source> (ex. oracle_neshu, yuman, gac)
allowed-tools: Bash(./dbt_venv/bin/dbt build:*), Bash(./dbt_venv/bin/dbt ls:*)
---

Build la source `$1` sur la cible **dev** (jamais `prod` sauf demande explicite).

Lance :

```bash
./dbt_venv/bin/dbt build --select tag:$1
```

- Si le tag `$1` ne correspond a aucun modele, liste les tags disponibles plutot que d'echouer en silence.
- Rapporte fidelement les echecs `run`/`test` avec leur output ; ne declare pas succes si un test est en erreur ou warning non attendu.
