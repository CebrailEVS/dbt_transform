
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select entity_type
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__stock_lcdp`
where entity_type is null



  
  
      
    ) dbt_internal_test