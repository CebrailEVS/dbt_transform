
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select task_id
from `evs-datastack-prod`.`prod_marts`.`fct_neshu__passage_appro`
where task_id is null



  
  
      
    ) dbt_internal_test