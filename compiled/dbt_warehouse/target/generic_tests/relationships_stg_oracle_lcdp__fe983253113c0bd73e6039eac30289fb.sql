
    
    

with child as (
    select idlocation as from_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__resources`
    where idlocation is not null
),

parent as (
    select idlocation as to_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__location`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


