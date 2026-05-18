
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_oracle_neshu__supply_flux`

where not(stock_total = stock_depot + stock_vehicule)


  
  
      
    ) dbt_internal_test