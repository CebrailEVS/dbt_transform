---
name: audit-sources
description: Audit dbt models against real BigQuery schemas. Detects missing columns, stale references, and schema drift between dbt SQL/YAML and actual BigQuery tables. Works on any layer (raw, staging, intermediate, marts) and can target a specific model.
argument-hint: "[source] [layer] [model] (e.g. 'oracle_neshu staging company')"
user-invocable: true
---

# Audit Sources — Schema Drift Detection

Compare dbt definitions against real BigQuery schemas to detect drift, missing columns, and stale references.

## Arguments

All arguments are optional and positional:

- `$0` — source name (e.g. `oracle_neshu`). If omitted, audit ALL sources.
- `$1` — layer to audit: `raw` (default), `staging`, `intermediate`, or `marts`
- `$2` — specific entity/model name (e.g. `company`, `device`). If omitted, audit all models in the layer.

### Usage examples

```
/audit-sources                                    → all sources, raw vs _sources.yml
/audit-sources oracle_neshu                       → one source, raw vs _sources.yml
/audit-sources oracle_neshu staging               → all staging models for oracle_neshu
/audit-sources oracle_neshu staging company       → just stg_oracle_neshu__company
/audit-sources oracle_neshu marts device          → just dim_oracle_neshu__device (or fct_)
/audit-sources yuman intermediate                 → all intermediate models for yuman
```

## Audit logic per layer

### Layer: `raw` (default)

Compare `_sources.yml` declarations and staging SQL against actual `prod_raw` tables.

| Check | Description |
|-------|-------------|
| **Missing in BQ** | Table declared in `_sources.yml` but not in `prod_raw` |
| **Missing in dbt** | Table in `prod_raw` (matching source prefix) but not declared |
| **No staging model** | Table declared in sources but no `stg_` SQL file exists |
| **Missing columns** | Column in BQ but not selected in staging model |
| **Extra columns** | Column referenced in staging but not in BQ (potential breakage) |
| **Undeclared columns** | Column in BQ, selected in staging, but not in `_sources.yml` |

**BigQuery dataset:** `prod_raw`
**dbt files:** `models/staging/<source>/_<source>__sources.yml` + `stg_<source>__*.sql`

### Layer: `staging`

Compare actual `prod_staging` tables in BigQuery against staging SQL model definitions.

| Check | Description |
|-------|-------------|
| **Column mismatch** | Column in BQ `prod_staging` table but not in the SQL select |
| **Extra in SQL** | Column in SQL but not materialized in BQ (build may be stale) |
| **Type mismatch** | Column type in BQ differs from what the SQL cast produces |

**BigQuery dataset:** `prod_staging`
**dbt files:** `models/staging/<source>/stg_<source>__<entity>.sql`
**Model name pattern:** `stg_<source>__<entity>`

### Layer: `intermediate`

Same checks as staging, against `prod_intermediate`.

**BigQuery dataset:** `prod_intermediate`
**dbt files:** `models/intermediate/<source>/int_<source>__<entity>.sql`
**Model name pattern:** `int_<source>__<entity>`

### Layer: `marts`

Same checks as staging, against `prod_marts`. Models can have `dim_` or `fct_` prefix.

**BigQuery dataset:** `prod_marts`
**dbt files:** `models/marts/<source>/dim_<source>__<entity>.sql` or `fct_<source>__<entity>.sql`
**Model name pattern:** `dim_<source>__<entity>` or `fct_<source>__<entity>`

## Steps

### 1. Parse arguments and determine scope

- Identify source(s), layer, and optional model filter
- Map the layer to the correct BigQuery dataset and dbt file paths

### 2. Gather BigQuery-side info

- Use `mcp__bigquery__get_table_info` for targeted model audits (one table)
- Use `mcp__bigquery__list_table_ids` + `get_table_info` for broader audits
- Project is always `evs-datastack-prod`

### 3. Gather dbt-side info

- Read the relevant SQL file(s) to understand which columns are selected/cast
- Read the relevant YAML file(s) for declared columns and tests

### 4. Cross-reference and report

Present results grouped by model:

```
## Audit: oracle_neshu / staging

### stg_oracle_neshu__company (prod_staging)
- OK: 10/10 columns match
- No drift detected

### stg_oracle_neshu__device (prod_staging)
- DRIFT: column `firmware_version` exists in BQ but not in SQL
- DRIFT: column `old_status` in SQL but not in BQ — potential breakage
- TYPE: `iddevice` is STRING in BQ, cast to INT64 in SQL — verify intent

### Summary
- Models checked: 18
- Clean: 15
- With drift: 3 (5 issues total)
```

## Important notes

- Use `mcp__bigquery__execute_sql_readonly` if `get_table_info` doesn't provide enough detail
- Ignore `_sdc_*` columns — they are Meltano system metadata
- The BigQuery project is always `evs-datastack-prod`
- Dataset mapping: raw → `prod_raw`, staging → `prod_staging`, intermediate → `prod_intermediate`, marts → `prod_marts`
- When auditing a specific model (`$2`), try both `dim_` and `fct_` prefixes for marts layer
- Be thorough but concise — flag issues, don't repeat OK columns
