
    
    

with child as (
    select ticket_id as from_field
    from `evs-datastack-prod`.`prod_staging`.`stg_zoho_desk__ticket_metrics`
    where ticket_id is not null
),

parent as (
    select ticket_id as to_field
    from `evs-datastack-prod`.`prod_staging`.`stg_zoho_desk__tickets`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


