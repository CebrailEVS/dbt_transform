
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select n_serie
from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__nunshen_stock_lot`
where n_serie is null



  
  
      
    ) dbt_internal_test