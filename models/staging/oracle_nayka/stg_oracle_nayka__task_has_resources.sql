{{
    config(
        materialized='table',
        description='Ressources affectées aux tâches Nayka (Oracle task_has_resources) : une ligne par tâche × ressource × statut d’affectation.'
    )
}}

with source_data as (
    select *
    from {{ source('oracle_nayka', 'nayka_task_has_resources') }}
),

cleaned_data as (
    -- Non reprises : quantity, cost, start_km, end_km, isreset (jamais renseignées),
    -- _parent_modification_date (curseur technique de dlt, copie de task.modification_date).
    select
        cast(idtask as int64) as idtask,
        cast(idresources as int64) as idresources,
        cast(task_status as int64) as task_status,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
