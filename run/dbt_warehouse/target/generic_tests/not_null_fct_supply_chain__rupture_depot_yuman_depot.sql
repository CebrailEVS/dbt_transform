
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select depot
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__rupture_depot_yuman`
where depot is null



  
  
      
    ) dbt_internal_test