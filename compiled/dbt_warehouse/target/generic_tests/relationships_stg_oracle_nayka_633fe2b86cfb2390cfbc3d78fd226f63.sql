
    
    

with child as (
    select idcontract as from_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__task_has_product`
    where idcontract is not null
),

parent as (
    select idcontract as to_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__contract`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


