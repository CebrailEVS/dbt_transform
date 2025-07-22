{{
    config(
        materialized='table',
        cluster_by=['idtask'],
        description='task_has_resources nettoyés et enrichis depuis evs_task_has_resources'
    )
}}

with source_data as (
    select *
    from {{ source('oracle_neshu', 'evs_task_has_resources') }}
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
)

select * from cleaned_data