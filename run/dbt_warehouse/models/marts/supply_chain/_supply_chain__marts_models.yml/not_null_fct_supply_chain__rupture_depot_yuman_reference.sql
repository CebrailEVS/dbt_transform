
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select reference
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__rupture_depot_yuman`
where reference is null



  
  
      
    ) dbt_internal_test