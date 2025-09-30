
    
    

with child as (
    select idcompany_customer as from_field
    from `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__device`
    where idcompany_customer is not null
),

parent as (
    select idcompany as to_field
    from `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__company`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


