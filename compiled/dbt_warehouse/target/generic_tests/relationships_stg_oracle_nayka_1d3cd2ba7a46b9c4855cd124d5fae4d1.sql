
    
    

with child as (
    select idunit_type as from_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__product_unit`
    where idunit_type is not null
),

parent as (
    select idunit_type as to_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__unit_type`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


