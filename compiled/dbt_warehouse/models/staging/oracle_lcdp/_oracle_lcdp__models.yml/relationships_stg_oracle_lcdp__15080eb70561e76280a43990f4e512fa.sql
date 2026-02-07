
    
    

with child as (
    select iddevice as from_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label_has_device`
    where iddevice is not null
),

parent as (
    select iddevice as to_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__device`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


