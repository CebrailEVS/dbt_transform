
    
    

with child as (
    select company_idcompany as from_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__company`
    where company_idcompany is not null
),

parent as (
    select idcompany as to_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__company`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


