
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__product_type`
      
    
    

    
    OPTIONS(
      description="""Types de produits Nayka (Oracle product_type) : produit, pi\u00e8ce, mod\u00e8le, ligne de prix\u2026"""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`nayka_product_type`
),

cleaned_data as (
    select
        cast(idproduct_type as int64) as idproduct_type,
        code,
        code_type,
        timestamp(datetime(modification_date), 'Europe/Paris') as updated_at,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
    );
  