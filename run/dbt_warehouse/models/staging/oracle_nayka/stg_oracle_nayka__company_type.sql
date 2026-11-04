
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__company_type`
      
    
    

    
    OPTIONS(
      description="""Types de soci\u00e9t\u00e9s Nayka (Oracle company_type) : client, fournisseur, d\u00e9p\u00f4t, agence\u2026"""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`nayka_company_type`
),

cleaned_data as (
    -- idstring renvoie à la table string, non extraite.
    select
        cast(idcompany_type as int64) as idcompany_type,
        code,
        code_type,
        timestamp(datetime(modification_date), 'Europe/Paris') as updated_at,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
    );
  