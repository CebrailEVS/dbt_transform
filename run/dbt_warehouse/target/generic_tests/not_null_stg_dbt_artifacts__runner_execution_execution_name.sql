
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select execution_name
from `evs-datastack-prod`.`prod_staging`.`stg_dbt_artifacts__runner_execution`
where execution_name is null



  
  
      
    ) dbt_internal_test