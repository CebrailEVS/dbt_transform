{{
    config(
        materialized='table',
        cluster_by=['idcontract'],
        description='Contrat nettoyés et enrichis depuis evs_contract'
    )
}}

with source_data as (
    select *
    from {{ source('oracle_neshu', 'evs_contract') }}
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idcontract as int64) as idcontract,
        cast(idcontract_type as int64) as idcontract_type,
        cast(idcompany_self as int64) as idcompany_self,
        cast(idcompany_financial as int64) as idcompany_financial,
        cast(idcompany_peer as int64) as idcompany_peer,
        cast(idcontact_creation as int64) as idcontact_creation,
        cast(idcontact_modification as int64) as idcontact_modification,
        
        -- Colonnes texte
        code,
        name,
        code_status_record,

        -- Colonnes XML
        xml,

        -- Date du contrat
        timestamp(original_start_date) as original_start_date,
        timestamp(original_end_date) as original_end_date,
        timestamp(current_end_date) as current_end_date,
        timestamp(termination_date) as termination_date,

        -- Timestamps harmonisés
        timestamp(creation_date) as created_at,
        timestamp(modification_date) as updated_at,
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at
        
    from source_data
)

select * from cleaned_data