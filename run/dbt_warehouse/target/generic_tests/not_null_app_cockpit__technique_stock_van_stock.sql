
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select stock
from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__technique_stock_van`
where stock is null



  
  
      
    ) dbt_internal_test