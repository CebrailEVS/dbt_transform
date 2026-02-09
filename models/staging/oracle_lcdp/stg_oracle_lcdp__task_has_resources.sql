{{
    config(
        materialized='table',
        description='task_has_resources nettoyés et enrichis depuis lcdp_task_has_resources'
    )
}}

with source_data as (
    select *
    from {{ source('oracle_lcdp', 'lcdp_task_has_resources') }}
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idtask as int64) as idtask,
        cast(idresources as int64) as idresources,
        cast(task_status as int64) as task_status,

        -- Timestamps harmonisés
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at

    from source_data
),

-- Synchro avec la table des tâches pour éviter les orphelins
filtered_data as (
    select cr.*
    from cleaned_data as cr
    inner join {{ ref('stg_oracle_lcdp__task') }} as t
        on cr.idtask = t.idtask
)

select * from filtered_data
