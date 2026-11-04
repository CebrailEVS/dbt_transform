
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select agent_id
from `evs-datastack-prod`.`prod_staging`.`stg_zoho_desk__agents`
where agent_id is null



  
  
      
    ) dbt_internal_test