---
description: Scaffold un nouveau mart (SQL + YAML) conforme a CONVENTIONS.md
argument-hint: <bu> <entity> (ex. supply_chain couverture_stock)
---

Cree un nouveau mart pour la BU `$1`, entite `$2`, en respectant **strictement** `CONVENTIONS.md` § Marts — pattern complet.

Procedure :

1. **Determiner le grain et le type** (dim vs fact) avec moi avant d'ecrire. Nom de fichier : `models/marts/$1/<dim|fct>_$1__$2.sql` (entite au singulier, snake_case, nom metier).
2. **Explorer l'upstream via le MCP BigQuery** avant d'ecrire : `get_table_info` pour le schema, `SELECT DISTINCT` pour les `accepted_values`, `COUNT(*)` pour les bornes `row_count_between`, `MIN/MAX(date)` pour les plages. Utiliser les schemas `prod_*` (pas `dev_*`, souvent perimes).
3. **Ecrire le SQL** : star schema strict (FK `<entity>_id` uniquement, pas de jointure fait-a-fait, pas de snowflake, pas d'OBT). Ordre des colonnes grain-first. `{{ config() }}` pour la materialisation uniquement (pas de description, pas de `tags`).
4. **Ecrire l'entree YAML** dans `_$1__marts_models.yml` : description en 4 blocs `[QUOI METIER]` / `[COMMENT CONSTRUITE]` / `[GRAIN]` / `[NOTES]` (grain obligatoire), + tests minimum (Dim : `unique`+`not_null` PK error, `accepted_values`/row count warn ; Fact : `not_null`+`relationships` FK, `unique_combination_of_columns`, `expression_is_true` invariants). Tests generiques sous `arguments:`.
5. **Mettre a jour l'exposure** `models/exposures/$1.yml` si un rapport Power BI consomme ce mart.
6. **Linter** : `./dbt_venv/bin/sqlfluff lint` sur le fichier avant de considerer termine.

Ne saute aucune couche : si un intermediate manque, le signaler plutot que de faire la logique metier dans le mart.
