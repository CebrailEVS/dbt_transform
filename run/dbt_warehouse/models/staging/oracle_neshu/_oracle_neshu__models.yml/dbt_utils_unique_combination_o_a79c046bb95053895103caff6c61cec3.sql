
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        idtask_type, idconfig
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_type_has_config`
    group by idtask_type, idconfig
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test