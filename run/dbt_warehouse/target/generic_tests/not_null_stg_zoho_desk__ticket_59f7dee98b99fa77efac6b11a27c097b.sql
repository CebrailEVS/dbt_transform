
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select _dlt_parent_id
from `evs-datastack-prod`.`prod_staging`.`stg_zoho_desk__ticket_history_event_info`
where _dlt_parent_id is null



  
  
      
    ) dbt_internal_test