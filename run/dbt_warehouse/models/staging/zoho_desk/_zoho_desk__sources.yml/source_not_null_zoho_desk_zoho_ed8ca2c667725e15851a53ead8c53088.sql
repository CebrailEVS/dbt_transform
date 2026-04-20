
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select _zoho_desk_associated_tickets_id
from `evs-datastack-prod`.`prod_raw`.`zoho_desk_ticket_metrics`
where _zoho_desk_associated_tickets_id is null



  
  
      
    ) dbt_internal_test