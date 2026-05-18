
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_oracle_neshu__supply_flux`

where not(stock_depot >= 0 and stock_vehicule >= 0)


  
  
      
    ) dbt_internal_test