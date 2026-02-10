
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_yuman__products`
      
    
    

    
    OPTIONS(
      description="""Produits transform\u00e9s et nettoy\u00e9s depuis l'API Yuman"""
    )
    as (
      

with source_data as (

    select *
    from `evs-datastack-prod`.`prod_raw`.`yuman_products`

),

cleaned_products as (

    select
        id as product_id,
        reference as product_code,
        designation as product_name,
        product_type,
        brand as product_brand,
        unit as product_unit,
        purchase_price as product_purchase_price,
        sale_price as product_sale_price,
        active as is_active,
        timestamp(created_at) as created_at,
        timestamp(updated_at) as updated_at,
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at
    from source_data
    where id is not null

)

select *
from cleaned_products
    );
  