
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select finished_at
from `evs-datastack-prod`.`prod_staging`.`stg_dbt_artifacts__runner_execution`
where finished_at is null



  
  
      
    ) dbt_internal_test