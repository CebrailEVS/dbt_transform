
    
    

with all_values as (

    select
        event_name as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_intermediate`.`int_zoho_desk__ticket_assignee_events`
    group by event_name

)

select *
from all_values
where value_field not in (
    'TicketUpdated','TicketCreated','TicketMergedMaster'
)


