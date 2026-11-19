
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select date_operation
from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__technique_mouvement_stock`
where date_operation is null



  
  
      
    ) dbt_internal_test