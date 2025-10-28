
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        task_id, roadman_code
    from `evs-datastack-prod`.`prod_marts`.`fct_oracle_neshu__passages_appro`
    group by task_id, roadman_code
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test