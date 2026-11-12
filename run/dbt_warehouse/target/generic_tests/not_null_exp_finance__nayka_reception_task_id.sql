
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select task_id
from `evs-datastack-prod`.`prod_export`.`exp_finance__nayka_reception`
where task_id is null



  
  
      
    ) dbt_internal_test