---
name: profile
description: Profile a BigQuery table from prod_raw before writing a dbt staging model. Shows column types, null rates, cardinality, sample values, and row count.
argument-hint: "<table_name> (e.g. evs_company or oracle_neshu.evs_company)"
user-invocable: true
---

# Profile a BigQuery Table

Run a data profiling analysis on a `prod_raw` table to understand its shape before writing a dbt model.

## Arguments

`$ARGUMENTS` — the table to profile. Accepts:
- Just the table name: `evs_company` (assumes `prod_raw` dataset)
- Full path: `oracle_neshu.evs_company` (source.table — still uses `prod_raw` dataset)

## Steps

### 1. Get table metadata

Use `mcp__bigquery__get_table_info` with:
- projectId: `evs-datastack-prod`
- datasetId: `prod_raw`
- tableId: the table name from `$ARGUMENTS`

Note the schema (column names and types), row count, and size.

### 2. Run profiling query

Use `mcp__bigquery__execute_sql_readonly` to run a single profiling query.

Build a query that computes for each column:
- **null_count** and **null_pct** — how many nulls
- **distinct_count** — cardinality
- **min / max** — for numeric and date/timestamp columns
- **sample values** — a few example values for string columns

Example pattern (adapt to actual columns):

```sql
select
    count(*) as total_rows,

    -- For each column:
    countif(column_name is null) as column_name__nulls,
    count(distinct column_name) as column_name__distinct,
    min(column_name) as column_name__min,
    max(column_name) as column_name__max
from `evs-datastack-prod.prod_raw.TABLE_NAME`
```

For tables with many columns, split into multiple queries if needed (BigQuery has output limits).

### 3. Get sample rows

```sql
select * from `evs-datastack-prod.prod_raw.TABLE_NAME` limit 5
```

### 4. Present the profile report

```
## Profile: prod_raw.evs_company
Rows: 1,234 | Size: 2.3 MB | Last modified: 2026-04-07

| Column | Type | Nulls | Null% | Distinct | Min | Max | Sample |
|--------|------|-------|-------|----------|-----|-----|--------|
| idcompany | INT64 | 0 | 0% | 1234 | 1 | 5678 | — |
| name | STRING | 12 | 1% | 1100 | — | — | "Acme Corp" |
| ...

### Observations
- column_x has 45% nulls — consider coalesce or filter
- column_y has only 3 distinct values — candidate for enum/label
- _sdc_extracted_at range: 2024-01-01 to 2026-04-07
```

### 5. Staging readiness recommendations

Based on the profile, suggest:
- Which columns to cast and to what type
- Which columns need coalesce for nulls
- Which columns look like foreign keys (naming pattern `id*`)
- Which columns are candidates for `created_at`, `updated_at` mapping
- Any data quality concerns (high null rates, low cardinality anomalies)

## Important notes

- Always use `mcp__bigquery__execute_sql_readonly` (never `execute_sql`)
- Limit profiling queries to avoid scanning too much data on large tables — use `limit` or sampling if row count > 1M
- Ignore `_sdc_*` columns in recommendations (they're system metadata) but do include them in the profile
- The output should help the user write a correct staging model on the first try
