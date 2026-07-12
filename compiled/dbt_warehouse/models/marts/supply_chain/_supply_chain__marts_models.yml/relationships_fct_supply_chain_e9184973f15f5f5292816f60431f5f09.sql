
    
    

with child as (
    select product_id as from_field
    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__prevision_demande_neshu`
    where product_id is not null
),

parent as (
    select product_id as to_field
    from `evs-datastack-prod`.`prod_marts`.`dim_neshu__product`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


