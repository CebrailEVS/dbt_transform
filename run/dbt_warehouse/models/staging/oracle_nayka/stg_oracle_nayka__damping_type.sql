
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__damping_type`
      
    
    

    
    OPTIONS(
      description="""Modes d'amortissement des \u00e9quipements Nayka (Oracle damping_type) : lin\u00e9aire, d\u00e9gressif, cr\u00e9dit-bail."""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`nayka_damping_type`
),

cleaned_data as (
    select
        cast(iddamping_type as int64) as iddamping_type,
        code,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
    );
  