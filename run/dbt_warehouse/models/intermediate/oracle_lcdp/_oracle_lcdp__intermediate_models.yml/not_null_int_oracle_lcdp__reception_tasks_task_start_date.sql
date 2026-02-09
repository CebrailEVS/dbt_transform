
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select task_start_date
from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__reception_tasks`
where task_start_date is null



  
  
      
    ) dbt_internal_test