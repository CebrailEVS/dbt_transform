
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        event_name as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_intermediate`.`int_zoho_desk__ticket_status_events`
    group by event_name

)

select *
from all_values
where value_field not in (
    'TicketUpdated','TicketCreated','TicketMergedMaster'
)



  
  
      
    ) dbt_internal_test