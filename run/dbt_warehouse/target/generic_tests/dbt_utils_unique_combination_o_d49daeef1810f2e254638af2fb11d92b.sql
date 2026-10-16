
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        idtask, idresources, task_status
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__task_has_resources`
    group by idtask, idresources, task_status
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test