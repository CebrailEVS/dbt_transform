
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        task_id, product_id
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__inter_technique_tasks`
    group by task_id, product_id
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test