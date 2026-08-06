
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__couverture_stock_neshu`

where not(conso_journaliere_n1 >= 0)


  
  
      
    ) dbt_internal_test