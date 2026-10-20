
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select is_jour_ouvre
from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__general_calendrier_ouvre`
where is_jour_ouvre is null



  
  
      
    ) dbt_internal_test