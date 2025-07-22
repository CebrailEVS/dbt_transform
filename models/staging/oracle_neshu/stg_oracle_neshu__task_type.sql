{{
    config(
        materialized='table',
        cluster_by=['idtask_type'],
        description='task_type nettoyés et enrichis depuis evs_task_type'
    )
}}

with source_data as (
    select *
    from {{ source('oracle_neshu', 'evs_task_type') }}
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idtask_type as int64) as idtask_type,

        -- Colonnes texte
        code,

        -- Timestamps harmonisés
        timestamp(modification_date) as updated_at,
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at
        
    from source_data
)

select * from cleaned_data