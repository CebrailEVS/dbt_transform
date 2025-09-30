
    
    

with child as (
    select idcompany as from_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__contact`
    where idcompany is not null
),

parent as (
    select idcompany as to_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__company`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


