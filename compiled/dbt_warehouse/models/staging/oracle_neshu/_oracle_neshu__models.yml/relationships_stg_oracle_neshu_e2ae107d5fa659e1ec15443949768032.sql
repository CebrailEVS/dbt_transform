
    
    

with child as (
    select idcompany_supplier as from_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__device`
    where idcompany_supplier is not null
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


