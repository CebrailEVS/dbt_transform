
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select idtask_type
from `evs-datastack-prod`.`prod_raw`.`lcdp_task_type`
where idtask_type is null



  
  
      
    ) dbt_internal_test