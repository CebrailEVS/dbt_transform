
    
    

with child as (
    select idcontact_modification as from_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__device`
    where idcontact_modification is not null
),

parent as (
    select idcontact as to_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__contact`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


