{{
    config(
        materialized='table',
        cluster_by=['idproduct'],
        description='Product nettoyés et enrichis depuis evs_product'
    )
}}

with source_data as (
    select *
    from {{ source('oracle_neshu', 'evs_product') }}
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idproduct as int64) as idproduct,
        cast(idproduct_type as int64) as idproduct_type,
        cast(idcompany_supplier as int64) as idcompany_supplier,
        cast(idcontact_creation as int64) as idcontact_creation,
        cast(idcontact_modification as int64) as idcontact_modification,
        
        -- Colonnes texte
        code,
        name,
        commercial_name,
        code_status_record,

        -- Colonnes numériques
        cast(purchase_unit_price as float64) as purchase_unit_price,
        cast(average_purchase_price as float64) as average_purchase_price,

        -- Timestamps harmonisés
        timestamp(creation_date) as created_at,
        timestamp(coalesce(modification_date, creation_date)) as updated_at, -- Use COALESCE to ensure updated_at is never null, falling back to creation_date
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at
        
    from source_data
)

select * from cleaned_data