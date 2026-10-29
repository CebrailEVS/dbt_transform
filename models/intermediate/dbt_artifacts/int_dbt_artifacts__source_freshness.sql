{{ config(materialized='view') }}

with executions as (
    select * from {{ ref('stg_dbt_artifacts__runner_execution') }}
),

source_checks as (
    select
        e.execution_name,
        e.freshness_selection,
        e.finished_at,
        source_check.unique_id,
        split(source_check.unique_id, '.')[safe_offset(2)] as source_name,
        split(source_check.unique_id, '.')[safe_offset(3)] as table_name,
        source_check.status,
        source_check.max_loaded_at,
        source_check.snapshotted_at as checked_at,
        source_check.max_loaded_at_time_ago_in_s / 3600 as age_hours
    from executions as e
    cross join unnest(e.freshness_results) as source_check
)

select * from source_checks
