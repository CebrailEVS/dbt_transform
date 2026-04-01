
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_yuman__storehouses`
      
    
    

    
    OPTIONS(
      description="""Storehouses clean depuis la table source yuman_storehouses"""
    )
    as (
      

with source_data as (

    select *
    from `evs-datastack-prod`.`prod_raw`.`yuman_storehouses`

),

cleaned_storehouses as (

    select
        id as storehouses_id,
        name as storehouses_name,
        address as storehouses_address,
        timestamp(_sdc_extracted_at) as extracted_at
    from source_data

)

select *
from cleaned_storehouses
    );
  