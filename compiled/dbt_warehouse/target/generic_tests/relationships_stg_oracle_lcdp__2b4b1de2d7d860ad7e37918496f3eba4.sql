
    
    

with child as (
    select idlabel_family as from_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label`
    where idlabel_family is not null
),

parent as (
    select idlabel_family as to_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label_family`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


