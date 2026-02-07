
    
    

with child as (
    select idcontact as from_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task`
    where idcontact is not null
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


