{{ 
  config(
    materialized='table',
    description='Produits/Articles nettoyés et enrichis depuis yuman_products',
  ) 
}}

with source_data as (
    select * 
    from {{ source('yuman_api', 'yuman_products') }}
),

cleaned_products as (
    select
        id as product_id,
        reference as product_code,
        designation as product_name,
        product_type as product_type,
        brand as product_brand,
        unit as product_unit,
        purchase_price as product_purchase_price,
        sale_price as product_sale_price,    
        active as is_active,
        TIMESTAMP(created_at) as created_at,
        TIMESTAMP(updated_at) as updated_at,
        TIMESTAMP(_sdc_extracted_at) as extracted_at,
        TIMESTAMP(_sdc_deleted_at) as deleted_at
    from source_data
    where id is not null
)

select * from cleaned_products