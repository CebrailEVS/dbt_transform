
    
    

with child as (
    select idlabel as from_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_has_device`
    where idlabel is not null
),

parent as (
    select idlabel as to_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


