{{ config(materialized='view') }}

with executions as (
    select * from {{ ref('stg_dbt_artifacts__runner_execution') }}
),

final as (
    select
        date(finished_at, 'Europe/Paris') as execution_date,
        execution_name as execution_id,
        -- source:yuman_api+ → yuman_api ; selector:<nom> → <nom> ; snapshots
        regexp_extract(selection, r'^(?:source:|selector:)?([^+]+)') as selection_name,
        selection,
        command,
        exit_code,
        {{ utc_vers_paris_affichage('started_at') }} as started_at,
        {{ utc_vers_paris_affichage('finished_at') }} as finished_at,
        exit_code = 0 as is_success,
        freshness_exit_code is not null as has_freshness_check,
        coalesce(freshness_exit_code, 0) != 0 as has_freshness_error,
        full_refresh as is_full_refresh,
        round(timestamp_diff(finished_at, started_at, second) / 60, 1) as duree_min
    from executions
)

select * from final
