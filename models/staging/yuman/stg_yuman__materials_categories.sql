{{ 
  config(
    materialized='table',
    description='Catégories (Machines) nettoyé depuis yuman_material_categories'
  ) 
}}

with source_data as (
    select * 
    from {{ source('yuman_api', 'yuman_material_categories') }}
),

cleaned_material_categories as (
    select
        id as category_id,
        name as category_name,   
        TIMESTAMP(created_at) as created_at,
        TIMESTAMP(updated_at) as updated_at,
        TIMESTAMP(_sdc_extracted_at) as extracted_at,
        TIMESTAMP(_sdc_deleted_at) as deleted_at
    from source_data
    where id is not null
)

select * from cleaned_material_categories