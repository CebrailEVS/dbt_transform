---
name: audit-docs
description: Audit dbt documentation quality of marts against real BigQuery data. Detects undocumented columns, undocumented enums, naming-convention violations (is_*/has_* non-boolean, *_date/*_at type mismatches), weak model descriptions (4-block trame / missing grain), and missing PK/FK tests. Produces a prioritized report with ready-to-paste YAML for mechanical fixes and templates for business meaning. Read-only — proposes, never edits.
argument-hint: "[bu] [layer] [model] (e.g. 'neshu marts consommation')"
user-invocable: true
---

# Audit Docs — Documentation Quality of the Consumed Layer

Close the virtuous loop: surface where the dbt documentation is incomplete or
inconsistent **vs the real BigQuery data**, so the data engineer knows exactly
what to document and where. This audits the *consumed* layer (marts first) —
the surface the DATA pole and Power BI actually read.

This skill **proposes**, it never edits models or YAML. For anything that needs
business meaning (what an enum value means, the grain), it emits a *template*
for the DE to fill — it must not invent semantics.

## Arguments

All optional, positional:

- `$0` — BU (e.g. `neshu`, `finance`). If omitted, audit ALL marts BUs.
- `$1` — layer: `marts` (default). `intermediate` / `staging` supported but
  lower priority (the consumed layer is marts).
- `$2` — specific model name (e.g. `consommation`, `company`). If omitted,
  audit all models in scope.

### Usage examples

```
/audit-docs                          → all marts, all BUs
/audit-docs neshu                    → all marts of the neshu BU
/audit-docs neshu marts consommation → just fct_neshu__consommation
/audit-docs finance                  → all marts of the finance BU
```

**BUs:** `technique`, `commerce`, `finance`, `services_generaux`, `supply_chain`,
`neshu`, `lcdp`.
**dbt files:** `models/marts/<bu>/{dim,fct}_<bu>__<entity>.sql` + `.yml`.
**BigQuery dataset:** `prod_marts` (project `evs-datastack-prod`).

## What it checks

Source of truth for every rule is `CONVENTIONS.md` (§ Nommage des marts,
§ Marts — pattern complet) and `CLAUDE.md`. Each finding gets a priority.

### P1 — Trust / correctness (mislead the SQL or the user)

| Check | How |
|---|---|
| **Undocumented enum** | low-cardinality STRING column (≤ 25 distinct) whose YAML has **no `accepted_values` test** AND whose description doesn't list the values. Detect real values via `distinct_values`. These are filter columns — undocumented = invented WHERE values. |
| **Boolean naming violation** | `is_*` / `has_*` column whose BQ type is **not BOOL** (e.g. `is_done` INT64). |
| **Date/time suffix violation** | `*_date` not `DATE`; `*_at` not `TIMESTAMP`. |
| **Missing grain** | model description has no `[GRAIN]` block / no "1 ligne par X" statement. |

### P2 — Completeness

| Check | How |
|---|---|
| **Undocumented column** | BQ column with no description in manifest/YAML (ignore system cols below). |
| **Weak model description** | missing, or not following the 4-block trame `[QUOI MÉTIER]` / `[COMMENT CONSTRUITE]` / `[GRAIN]` / `[NOTES]`. |
| **Missing PK tests (dim)** | `dim_` without `unique` + `not_null` on its PK (`<entity>_id`). |
| **Missing FK tests (fct)** | `fct_` FK column (`*_id`) without a `relationships` test. |

### P3 — Data smells (flag, don't block)

| Check | How |
|---|---|
| **Inconsistent categoricals** | mixed casing / near-duplicate values in an enum (e.g. `categorie_machine`). |
| **Suspicious values** | obvious typos in distinct values (e.g. `Tâche dactivité`). |

## Steps

### 1. Resolve scope
Map BU(s) → marts models (read `models/marts/<bu>/` for SQL + YAML file names).

### 2. Read current docs (the anti-drift step)
Prefer `target/manifest.json` + `target/catalog.json` — descriptions live there
via `persist_docs`, and catalog has every column + BQ type. Fall back to the
`.yml` files. **Always read the current state** so you never propose a fix that
already exists.

### 3. Gather BQ reality
- Columns + types: from `catalog.json`, or `mcp__bigquery__get_table_info`.
- Enum values: `mcp__bigquery__execute_sql_readonly` with
  `SELECT <col> AS value, count(*) n FROM prod_marts.<table> GROUP BY value ORDER BY n DESC LIMIT 30`.
  Only probe STRING columns that look categorical (skip free-text, ids, names).

### 4. Cross-reference, prioritize, and report
Group by table, P1 → P3. For each finding: **what** / **why (which convention)**
/ **suggested fix**. Split fixes into:
- **Mécanique** — you write the ready-to-paste YAML (column description for an
  obvious field, `accepted_values` with the real values you observed, a test block).
- **Sens métier** — you emit a *template* with the observed facts (e.g. "enum
  `resp` = {0, 50, 100} — signification ? remplir la description") for the DE.

## Output format

```
# Audit docs — <scope>

## fct_neshu__consommation (prod_marts)

### P1
- [enum non documenté] `data_source` = {TELEMETRIE, CHARGEMENT, LIVRAISON} — aucun accepted_values.
  → Mécanique, à coller :
      - name: data_source
        description: "Source de la consommation : TELEMETRIE | CHARGEMENT | LIVRAISON."
        tests:
          - accepted_values:
              arguments:
                values: ['TELEMETRIE', 'CHARGEMENT', 'LIVRAISON']
- [grain manquant] la description n'a pas de bloc [GRAIN].
  → Template : "[GRAIN] 1 ligne par ____ (à préciser)."

### P2
- [colonne sans description] `location` — non documentée.
  → Template (sens métier) : décrire `location`.

## Résumé
- Modèles audités : N
- Trous P1 : x · P2 : y · P3 : z
- Top 3 à traiter en premier : ...
```

## Important notes

- **Read-only.** Never edit a model, YAML, or run a write. Propose only.
- **Never invent business meaning.** Enum semantics, grain, ambiguous columns →
  template for the DE, not a guessed description.
- **Ignore system columns**: `dbt_updated_at`, `dbt_invocation_id`, `_sdc_*`,
  and the standard staging timestamps when auditing upstream.
- Cap enum probes at cardinality ≤ 25 and skip obvious free-text / id / name /
  code columns (high cardinality) — they aren't enums.
- Generic test args nest under `arguments:` (dbt ≥ 1.11), per `CONVENTIONS.md`.
- Marts descriptions live in **YAML only** (not the SQL config block) — propose
  YAML edits, never `{{ config(description=...) }}` for marts.
- Project is always `evs-datastack-prod`. Be thorough but concise — flag gaps,
  don't list what's already fine.
- This finds **where** to document; the DE provides **what**. That division is
  the point of the loop.
