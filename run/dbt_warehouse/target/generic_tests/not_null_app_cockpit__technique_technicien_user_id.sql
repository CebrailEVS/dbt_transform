
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select user_id
from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__technique_technicien`
where user_id is null



  
  
      
    ) dbt_internal_test