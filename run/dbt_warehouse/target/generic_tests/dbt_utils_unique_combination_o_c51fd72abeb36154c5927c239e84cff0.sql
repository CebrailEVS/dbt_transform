
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        periode, evt_id
    from `evs-datastack-prod`.`prod_staging`.`stg_apptech__suivi_tech_events`
    group by periode, evt_id
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test