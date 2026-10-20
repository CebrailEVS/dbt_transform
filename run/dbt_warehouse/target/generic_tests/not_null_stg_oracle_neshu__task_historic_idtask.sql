
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select idtask
from (select * from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_historic` where idhistoric_action not in (129, 130, 131)) dbt_subquery
where idtask is null



  
  
      
    ) dbt_internal_test