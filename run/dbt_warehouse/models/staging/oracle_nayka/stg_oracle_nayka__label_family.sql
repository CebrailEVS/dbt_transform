
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__label_family`
      
    
    

    
    OPTIONS(
      description="""Familles d'\u00e9tiquettes Nayka (Oracle label_family) : axe de classement pos\u00e9 sur les soci\u00e9t\u00e9s, machines, produits, ressources, contrats et t\u00e2ches."""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`nayka_label_family`
),

cleaned_data as (
    select
        cast(idlabel_family as int64) as idlabel_family,
        cast(idstring as int64) as idstring,
        code,
        cast(exclus as int64) as exclus,
        cast(required as int64) as required,
        cast(system as int64) as system,  -- noqa: RF04
        timestamp(datetime(modification_date), 'Europe/Paris') as updated_at,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
    );
  