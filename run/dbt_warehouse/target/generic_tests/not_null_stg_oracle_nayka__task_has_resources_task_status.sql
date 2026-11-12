
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select task_status
from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__task_has_resources`
where task_status is null



  
  
      
    ) dbt_internal_test