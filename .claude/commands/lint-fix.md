---
description: Corrige automatiquement le SQL (sqlfluff fix) puis relint pour confirmer
argument-hint: <path> (fichier .sql ou dossier sous models/)
allowed-tools: Bash(./dbt_venv/bin/sqlfluff fix:*), Bash(./dbt_venv/bin/sqlfluff lint:*)
---

Corrige puis verifie le SQL de `$1` (regles SQLFluff v4 : lowercase, indent 4, max 120, pas de trailing comma).

```bash
./dbt_venv/bin/sqlfluff fix $1
./dbt_venv/bin/sqlfluff lint $1
```

Liste les violations restantes que `fix` n'a pas pu corriger automatiquement (a regler a la main). Ne considere le fichier propre que si le relint final ne renvoie aucune erreur.
