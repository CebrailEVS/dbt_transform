{{
    config(
        materialized='table',
        description='label_has_resources nettoyés et enrichis depuis evs_label_has_resources'
    )
}}

with source_data as (
    select *
    from {{ source('oracle_neshu', 'evs_label_has_resources') }}
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idlabel as int64) as idlabel,
        cast(idresources as int64) as idresources,

        -- Timestamps harmonisés
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at

    from source_data
)

select * from cleaned_data
