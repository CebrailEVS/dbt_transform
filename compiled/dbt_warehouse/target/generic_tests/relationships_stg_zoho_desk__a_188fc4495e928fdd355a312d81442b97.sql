
    
    

with child as (
    select _dlt_parent_id as from_field
    from `evs-datastack-prod`.`prod_staging`.`stg_zoho_desk__agent_departments`
    where _dlt_parent_id is not null
),

parent as (
    select _dlt_id as to_field
    from `evs-datastack-prod`.`prod_staging`.`stg_zoho_desk__agents`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


