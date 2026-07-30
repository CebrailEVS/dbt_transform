{{
    config(
        materialized='table',
        description='label_has_product nettoyés et enrichis depuis lcdp_label_has_product'
    )
}}

with source_data as (
    select *
    from {{ source('oracle_lcdp', 'lcdp_label_has_product') }}
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idlabel as int64) as idlabel,
        cast(idproduct as int64) as idproduct,

        -- Timestamps harmonisés
        timestamp(_extracted_at) as extracted_at

    from source_data
)

select * from cleaned_data
