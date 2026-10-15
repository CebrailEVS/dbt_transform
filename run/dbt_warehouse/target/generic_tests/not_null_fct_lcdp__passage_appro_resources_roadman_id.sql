
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select resources_roadman_id
from `evs-datastack-prod`.`prod_marts`.`fct_lcdp__passage_appro`
where resources_roadman_id is null



  
  
      
    ) dbt_internal_test