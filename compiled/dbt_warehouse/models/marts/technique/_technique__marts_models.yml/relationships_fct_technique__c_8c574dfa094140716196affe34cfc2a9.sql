
    
    

with child as (
    select key_inter_fautive as from_field
    from `evs-datastack-prod`.`prod_marts`.`fct_technique__credit_repair_warranty`
    where key_inter_fautive is not null
),

parent as (
    select key_inter as to_field
    from `evs-datastack-prod`.`prod_marts`.`fct_technique__intervention`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


