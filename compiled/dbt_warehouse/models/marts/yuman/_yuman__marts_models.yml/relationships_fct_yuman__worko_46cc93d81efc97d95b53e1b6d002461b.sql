
    
    

with child as (
    select material_id as from_field
    from `evs-datastack-prod`.`prod_marts`.`fct_yuman__workorder_pricing`
    where material_id is not null
),

parent as (
    select material_id as to_field
    from `evs-datastack-prod`.`prod_marts`.`dim_yuman__materials`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


