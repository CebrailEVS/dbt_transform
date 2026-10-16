
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select task_start_date
from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__lcdp_chargement`
where task_start_date is null



  
  
      
    ) dbt_internal_test