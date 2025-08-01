{{
    config(
        materialized='table',
        cluster_by=['idlocation'],
        description='company_has_location nettoyés et enrichis depuis evs_company_has_location'
    )
}}

with source_data as (
    select *
    from {{ source('oracle_neshu', 'evs_company_has_location') }}
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idlocation as int64) as idlocation,
        cast(idcompany as int64) as idcompany,
        cast(idlocation_type as int64) as idlocation_type,

        -- Timestamps harmonisés
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at
        
    from source_data
)

select * from cleaned_data