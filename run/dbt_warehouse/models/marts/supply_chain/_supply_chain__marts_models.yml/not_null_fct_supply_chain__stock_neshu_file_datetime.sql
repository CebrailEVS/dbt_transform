
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select file_datetime
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__stock_neshu`
where file_datetime is null



  
  
      
    ) dbt_internal_test