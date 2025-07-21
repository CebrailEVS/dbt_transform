{{
    config(
        materialized='table',
        cluster_by=['idlabel'],
        description='label_has_device nettoyés et enrichis depuis evs_label_has_device'
    )
}}

with source_data as (
    select *
    from {{ source('oracle_neshu', 'evs_label_has_device') }}
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idlabel as int64) as idlabel,
        cast(iddevice as int64) as iddevice,

        -- Timestamps harmonisés
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at
        
    from source_data
)

select * from cleaned_data