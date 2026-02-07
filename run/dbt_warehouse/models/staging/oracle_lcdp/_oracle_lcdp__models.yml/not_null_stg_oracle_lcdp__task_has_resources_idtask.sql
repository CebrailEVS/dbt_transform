
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select idtask
from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task_has_resources`
where idtask is null



  
  
      
    ) dbt_internal_test