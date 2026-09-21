
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select mc
from `evs-datastack-prod`.`prod_marts`.`fct_commerce__machine_intervention`
where mc is null



  
  
      
    ) dbt_internal_test