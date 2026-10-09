
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select id_entity
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__stock_lcdp`
where id_entity is null



  
  
      
    ) dbt_internal_test