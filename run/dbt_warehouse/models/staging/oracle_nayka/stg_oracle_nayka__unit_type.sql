
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__unit_type`
      
    
    

    
    OPTIONS(
      description="""Types d'unit\u00e9s de produit Nayka (Oracle unit_type) : achat, vente, stock, r\u00e9appro\u2026"""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`nayka_unit_type`
),

cleaned_data as (
    select
        cast(idunit_type as int64) as idunit_type,
        code,
        code_type,
        timestamp(datetime(modification_date), 'Europe/Paris') as updated_at,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
    );
  