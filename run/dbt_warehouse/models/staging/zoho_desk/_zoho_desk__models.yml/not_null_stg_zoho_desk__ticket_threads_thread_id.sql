
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select thread_id
from `evs-datastack-prod`.`prod_staging`.`stg_zoho_desk__ticket_threads`
where thread_id is null



  
  
      
    ) dbt_internal_test