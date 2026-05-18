
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_yuman__storehouses`
      
    
    

    
    OPTIONS(
      description="""Storehouses clean depuis la table source yuman_storehouses. R\u00e8gle m\u00e9tier : storehouses_id correspond au user_id du technicien ou manager propri\u00e9taire de l'entrep\u00f4t mobile. Exception : les ateliers physiques (Rungis/Lyon, Lyon, Strasbourg, Bordeaux) n'ont pas de user associ\u00e9 \u2014 4 enregistrements concern\u00e9s.\n"""
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
  