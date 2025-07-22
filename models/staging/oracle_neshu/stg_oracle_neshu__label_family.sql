{{
    config(
        materialized='table',
        cluster_by=['idlabel_family'],
        description='Label famille nettoyés et enrichis depuis evs_label_family'
    )
}}

with source_data as (
    select *
    from {{ source('oracle_neshu', 'evs_label_family') }}
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idlabel_family as int64) as idlabel_family,
        
        -- Colonnes texte
        code,

        -- Timestamps harmonisés
        timestamp(modification_date) as updated_at,
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at
        
    from source_data
)

select * from cleaned_data