
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__flux_neshu`

where not(stock_total = stock_depot + stock_vehicule)


  
  
      
    ) dbt_internal_test