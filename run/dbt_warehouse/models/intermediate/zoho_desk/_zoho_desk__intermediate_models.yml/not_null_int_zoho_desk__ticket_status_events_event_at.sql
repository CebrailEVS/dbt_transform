
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select event_at
from `evs-datastack-prod`.`prod_intermediate`.`int_zoho_desk__ticket_status_events`
where event_at is null



  
  
      
    ) dbt_internal_test