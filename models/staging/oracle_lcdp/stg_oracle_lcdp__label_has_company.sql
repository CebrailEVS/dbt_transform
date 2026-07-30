{{
    config(
        materialized='table',
        description='label_has_company nettoyés et enrichis depuis lcdp_label_has_company'
    )
}}

with source_data as (
    select *
    from {{ source('oracle_lcdp', 'lcdp_label_has_company') }}
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idlabel as int64) as idlabel,
        cast(idcompany as int64) as idcompany,

        -- Timestamps harmonisés
        timestamp(_extracted_at) as extracted_at

    from source_data
)

select * from cleaned_data
