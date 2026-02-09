{{
    config(
        materialized='table',
        description='Resources nettoyés et enrichis depuis lcdp_resources'
    )
}}

with source_data as (
    select *
    from {{ source('oracle_lcdp', 'lcdp_resources') }}
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idresources as int64) as idresources,
        cast(idresources_type as int64) as idresources_type,
        cast(resources_idresources as int64) as resources_idresources,
        cast(idcompany as int64) as idcompany,
        cast(idcompany_storehouse as int64) as idcompany_storehouse,
        cast(idlocation as int64) as idlocation,
        cast(idcontact as int64) as idcontact,
        cast(idcontact_creation as int64) as idcontact_creation,
        cast(idcontact_modification as int64) as idcontact_modification,

        -- Colonnes texte
        code,
        name,
        code_status_record,

        -- Colonnes numériques
        cast(cost as float64) as cost,

        -- Timestamps harmonisés
        timestamp(arrival) as arrival,
        timestamp(departure) as departure,

        -- Timestamps harmonisés
        timestamp(creation_date) as created_at,
        -- Use COALESCE to ensure updated_at is never null, falling back to creation_date
        timestamp(coalesce(modification_date, creation_date)) as updated_at,
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at

    from source_data
)

select * from cleaned_data
