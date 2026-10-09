
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select entity_type
from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__lcdp_stock_photo`
where entity_type is null



  
  
      
    ) dbt_internal_test