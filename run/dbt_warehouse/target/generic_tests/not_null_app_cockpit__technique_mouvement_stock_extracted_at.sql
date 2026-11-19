
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select extracted_at
from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__technique_mouvement_stock`
where extracted_at is null



  
  
      
    ) dbt_internal_test