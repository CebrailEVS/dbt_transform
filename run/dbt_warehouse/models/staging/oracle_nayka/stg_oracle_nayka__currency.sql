
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__currency`
      
    
    

    
    OPTIONS(
      description="""Devises Nayka (Oracle currency) : r\u00e9f\u00e9rentiel ISO 4217 livr\u00e9 avec l'ERP, seul EUR est actif."""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`nayka_currency`
),

cleaned_data as (
    select
        cast(idcurrency as int64) as idcurrency,
        code,
        symbole,
        cast(isactif as int64) as isactif,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
    );
  