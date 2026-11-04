
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select workspace_id
from `evs-datastack-prod`.`prod_staging`.`stg_powerbi_activity__datasets`
where workspace_id is null



  
  
      
    ) dbt_internal_test