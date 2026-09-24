
    
    

with child as (
    select idstring as from_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label_family`
    where idstring is not null
),

parent as (
    select idstring as to_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__string`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


