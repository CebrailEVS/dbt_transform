
    
    

with child as (
    select workorder_id as from_field
    from `evs-datastack-prod`.`prod_staging`.`stg_yuman__workorder_products`
    where workorder_id is not null
),

parent as (
    select workorder_id as to_field
    from `evs-datastack-prod`.`prod_staging`.`stg_yuman__workorders`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


