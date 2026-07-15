
    
    

with child as (
    select company_id as from_field
    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__erreur_prevision_neshu`
    where company_id is not null
),

parent as (
    select company_id as to_field
    from `evs-datastack-prod`.`prod_marts`.`dim_neshu__company`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


