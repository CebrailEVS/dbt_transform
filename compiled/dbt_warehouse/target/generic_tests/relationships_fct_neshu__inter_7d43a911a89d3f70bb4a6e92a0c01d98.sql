
    
    

with child as (
    select piece_id as from_field
    from `evs-datastack-prod`.`prod_marts`.`fct_neshu__intervention_technique`
    where piece_id is not null
),

parent as (
    select piece_id as to_field
    from `evs-datastack-prod`.`prod_marts`.`dim_neshu__piece`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


