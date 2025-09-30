
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select idtask_status
from `evs-datastack-prod`.`prod_raw`.`evs_task_status`
where idtask_status is null



  
  
      
    ) dbt_internal_test