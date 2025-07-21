{{
    config(
        materialized='table',
        cluster_by=['idlabel','idlabel_family'],
        description='Label nettoyés et enrichis depuis evs_label'
    )
}}

with source_data as (
    select *
    from {{ source('oracle_neshu', 'evs_label') }}
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idlabel as int64) as idlabel,
        cast(idlabel_family as int64) as idlabel_family,
        
        -- Colonnes texte
        code,

        -- Bolean
        cast(system as boolean) as is_system,
        cast(enabled as boolean) as is_enabled,

        -- Timestamps harmonisés
        timestamp(modification_date) as updated_at,
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at
        
    from source_data
)

select * from cleaned_data