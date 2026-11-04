
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__label_has_contract`
      
    
    

    
    OPTIONS(
      description="""\u00c9tiquettes pos\u00e9es sur les contrats Nayka (Oracle label_has_contract) : une ligne par contrat \u00d7 \u00e9tiquette."""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`nayka_label_has_contract`
),

cleaned_data as (
    select
        cast(idcontract as int64) as idcontract,
        cast(idlabel as int64) as idlabel,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
    );
  