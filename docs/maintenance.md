# Maintenance — nettoyage des objets orphelins

Cette doc couvre le nettoyage des **objets orphelins** dans BigQuery : tables/vues
qui existent encore dans un dataset mais ne correspondent plus à aucun modèle dbt
(séquelles d'une suppression ou d'un renommage de modèle). dbt ne les supprime
jamais tout seul — d'où cet outillage.

## Outil — `dbt_orphan`

Package : [`Matts52/dbt-orphan`](https://github.com/Matts52/dbt-orphan) (installé via
git dans `packages.yml`, pin `v0.1.3` — **pas publié sur dbt Hub**).

Comment il fonctionne : il liste `INFORMATION_SCHEMA.TABLES` du dataset scanné, puis
flague tout objet dont le nom n'est pas un `model`/`seed`/`snapshot` du graph dbt.
Les **snapshots sont reconnus** par le graph, donc protégés.

## Procédure

> **Toujours commencer en `dry_run: true`** (lecture seule, aucun `DROP`), vérifier
> la liste, puis seulement décider.

### 1. Auditer (dry-run, lecture seule)

```bash
# prod — scanner les datasets voulus
./dbt_venv/bin/dbt run-operation dbt_orphan.cleanup_orphans \
  --args '{schemas: ["prod_marts", "prod_staging", "prod_intermediate"], dry_run: true}' \
  --target prod

# dev (target par défaut)
./dbt_venv/bin/dbt run-operation dbt_orphan.cleanup_orphans \
  --args '{schemas: ["dev_marts", "dev_staging", "dev_intermediate"], dry_run: true}'
```

### 2. Vérifier avant de supprimer

Avant tout `dry_run: false`, confirmer qu'aucun rapport Power BI (exposure) ni
l'application (`evs-app`) ne consomme encore l'objet listé.

### 3. Supprimer (cleanup réel)

```bash
./dbt_venv/bin/dbt run-operation dbt_orphan.cleanup_orphans \
  --args '{schemas: ["prod_marts"], dry_run: false}' --target prod
```

### Variante — audit en modèle

`get_orphans()` matérialise la liste des orphelins dans une table d'analyse, sans
rien dropper :

```sql
-- models/_analysis/orphaned_objects.sql
{{ dbt_orphan.get_orphans(schemas=['prod_marts'], exclude_patterns=[], include_patterns=[]) }}
```

## Arguments

| Arg | Défaut | Rôle |
|---|---|---|
| `schemas` | `[target.schema]` | datasets à scanner (liste — à passer explicitement) |
| `dry_run` | `true` | `true` = log seulement · `false` = `DROP` |
| `database` | `target.database` | projet BQ (auto) |
| `exclude_patterns` | `[]` | protège des tables (LIKE SQL, ex. `["snap_%"]`) |
| `include_patterns` | `[]` | ne cible que ces tables (LIKE SQL) |

## Règles d'usage

- **`dry_run: true` d'abord, systématiquement.** Cf. hard rule « never drop unless
  explicitly asked ».
- **Aligner le target sur le dataset scanné** : le macro compare les noms aux nodes
  dont `node.schema` == schema scanné. Scanner `prod_marts` depuis le target `dev`
  → faux positifs massifs (aucun node ne résout vers `prod_marts`).
- **Jamais en post-hook automatique** ni dans un workflow planifié. Nettoyage
  toujours manuel et validé.
- **Snapshots** : déjà protégés par le graph ; en cleanup réel sur le dataset
  `snapshots`, ajouter `exclude_patterns: ["snap_%"]` par sécurité (perte SCD2
  irréversible).
- **Limite connue** : le macro compare `node.name`, pas l'`alias`. Un modèle avec un
  `alias` ≠ nom de fichier serait faussement vu comme orphelin (les alias actuels
  sont no-op, donc OK).
