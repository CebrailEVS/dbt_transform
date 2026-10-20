{{
    config(
        materialized='table',
        description='Actions de l''historique des tâches depuis evs_historic_action'
    )
}}

with source_data as (
    select *
    from {{ source('oracle_neshu', 'evs_historic_action') }}
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idhistoric_action as int64) as idhistoric_action,
        cast(idstring as int64) as idstring,

        -- Colonnes texte
        code,

        -- Timestamps harmonisés
        {{ paris_vers_utc('modification_date') }} as updated_at,
        timestamp(_extracted_at) as extracted_at

    from source_data
)

select * from cleaned_data
