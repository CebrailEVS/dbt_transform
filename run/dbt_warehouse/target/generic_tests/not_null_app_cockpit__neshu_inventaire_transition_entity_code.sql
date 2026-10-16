
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select entity_code
from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__neshu_inventaire_transition`
where entity_code is null



  
  
      
    ) dbt_internal_test