
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_technique__repair`

where not(is_repair = (episode_rank > 1))


  
  
      
    ) dbt_internal_test