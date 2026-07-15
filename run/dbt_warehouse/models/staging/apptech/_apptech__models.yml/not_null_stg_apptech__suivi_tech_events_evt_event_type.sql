
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select evt_event_type
from `evs-datastack-prod`.`prod_staging`.`stg_apptech__suivi_tech_events`
where evt_event_type is null



  
  
      
    ) dbt_internal_test