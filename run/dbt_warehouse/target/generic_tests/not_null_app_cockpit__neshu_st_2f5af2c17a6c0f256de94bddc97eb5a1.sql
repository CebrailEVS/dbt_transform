
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select snapshot_date_veille
from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__neshu_stock_disparition`
where snapshot_date_veille is null



  
  
      
    ) dbt_internal_test