
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select _dlt_id
from `evs-datastack-prod`.`prod_raw`.`zoho_desk_ticket_history`
where _dlt_id is null



  
  
      
    ) dbt_internal_test