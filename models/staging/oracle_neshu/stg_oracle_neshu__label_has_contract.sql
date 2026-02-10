{{
    config(
        materialized='table',
        description='label_has_contract nettoyés et enrichis depuis evs_label_has_contract'
    )
}}

with source_data as (
    select *
    from {{ source('oracle_neshu', 'evs_label_has_contract') }}
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idlabel as int64) as idlabel,
        cast(idcontract as int64) as idcontract,

        -- Timestamps harmonisés
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at

    from source_data
)

select * from cleaned_data
