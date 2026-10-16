{{
    config(
        materialized='table',
        description='Statuts des tâches Nayka (Oracle task_status) : un code par statut (PREVU, FAIT, VALIDE…).'
    )
}}

with source_data as (
    select *
    from {{ source('oracle_nayka', 'nayka_task_status') }}
),

cleaned_data as (
    -- idstring et idstring_action renvoient à la table string, non extraite.
    select
        cast(idtask_status as int64) as idtask_status,
        code,
        {{ paris_vers_utc('modification_date') }} as updated_at,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
