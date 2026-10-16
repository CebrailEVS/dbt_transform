
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__label_family`
      
    
    

    
    OPTIONS(
      description=""""""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`nayka_label_family`
),

cleaned_data as (
    -- Retirées : idstring (table string non extraite), idinstance (constante),
    -- export_mobile et required_em (réglages de l'application mobile).
    select
        cast(idlabel_family as int64) as idlabel_family,
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
  