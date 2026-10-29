
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        execution_name, unique_id
    from `evs-datastack-prod`.`prod_intermediate`.`int_dbt_artifacts__source_freshness`
    group by execution_name, unique_id
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test