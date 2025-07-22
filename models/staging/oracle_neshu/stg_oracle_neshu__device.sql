{{
    config(
        materialized='table',
        cluster_by=['iddevice','idlocation','idcompany_customer'],
        description='Device nettoyés et enrichis depuis evs_device'
    )
}}

with source_data as (
    select *
    from {{ source('oracle_neshu', 'evs_device') }}
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(iddevice as int64) as iddevice,
        cast(device_iddevice as int64) as device_iddevice,
        cast(idcompany_customer as int64) as idcompany_customer,
        cast(idcompany_supplier as int64) as idcompany_supplier,
        cast(idcompany_owner as int64) as idcompany_owner,
        cast(idcontact_creation as int64) as idcontact_creation,
        cast(idcontact_modification as int64) as idcontact_modification,
        cast(idlocation as int64) as idlocation,
        
        -- Colonnes texte
        code,
        name,
        serial,
        code_status_record,

        -- Date liée à la machine
        timestamp(last_installation_date) as last_installation_date,
        timestamp(purchase_date) as purchase_date,

        -- Timestamps harmonisés
        timestamp(creation_date) as created_at,
        timestamp(coalesce(modification_date, creation_date)) as updated_at, -- Use COALESCE to ensure updated_at is never null, falling back to creation_date
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at
        
    from source_data
)

select * from cleaned_data