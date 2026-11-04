
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select execution_id
from `evs-datastack-prod`.`prod_marts`.`dim_plateforme__execution_dbt`
where execution_id is null



  
  
      
    ) dbt_internal_test