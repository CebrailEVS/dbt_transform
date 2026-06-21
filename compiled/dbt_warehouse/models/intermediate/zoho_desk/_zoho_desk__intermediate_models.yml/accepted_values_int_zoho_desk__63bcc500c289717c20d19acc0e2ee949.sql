
    
    

with all_values as (

    select
        actor_type as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_intermediate`.`int_zoho_desk__ticket_priority_events`
    group by actor_type

)

select *
from all_values
where value_field not in (
    'Agent','Contact','Workflow'
)


