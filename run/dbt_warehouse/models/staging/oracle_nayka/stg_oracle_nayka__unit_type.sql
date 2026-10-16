
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__unit_type`
      
    
    

    
    OPTIONS(
      description=""""""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`nayka_unit_type`
),

cleaned_data as (
    -- idstring renvoie à la table string, non extraite.
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
  