
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select idtask
from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_has_task`
where idtask is null



  
  
      
    ) dbt_internal_test