{{
    config(
        materialized='table',
        description='Label famille nettoyés et enrichis depuis lcdp_label_family'
    )
}}

with source_data as (
    select *
    from {{ source('oracle_lcdp', 'lcdp_label_family') }}
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idlabel_family as int64) as idlabel_family,

        -- Colonnes texte
        code,

        -- Timestamps harmonisés
        timestamp(modification_date) as updated_at,
        timestamp(_extracted_at) as extracted_at

    from source_data
)

select * from cleaned_data
