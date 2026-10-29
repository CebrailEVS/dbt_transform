{{ config(materialized='view') }}

with source_checks as (
    select * from {{ ref('int_dbt_artifacts__source_freshness') }}
),

final as (
    select
        date(finished_at, 'Europe/Paris') as execution_date,
        execution_name as execution_id,
        source_name,
        table_name,
        status,
        {{ utc_vers_paris_affichage('max_loaded_at') }} as max_loaded_at,
        {{ utc_vers_paris_affichage('checked_at') }} as checked_at,
        age_hours as age_heures
    from source_checks
)

select * from final
