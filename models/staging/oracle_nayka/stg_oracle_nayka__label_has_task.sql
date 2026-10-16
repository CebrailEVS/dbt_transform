{{
    config(
        materialized='table',
        description='Étiquettes posées sur les tâches Nayka (Oracle label_has_task) : une ligne par tâche × étiquette.'
    )
}}

with source_data as (
    select *
    from {{ source('oracle_nayka', 'nayka_label_has_task') }}
),

cleaned_data as (
    -- _parent_modification_date non reprise : curseur technique de dlt, copie de task.modification_date.
    select
        cast(idtask as int64) as idtask,
        cast(idlabel as int64) as idlabel,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
