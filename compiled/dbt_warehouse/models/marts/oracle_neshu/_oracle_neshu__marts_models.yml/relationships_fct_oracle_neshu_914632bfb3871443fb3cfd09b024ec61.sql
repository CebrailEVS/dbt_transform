
    
    

with child as (
    select company_id as from_field
    from `evs-datastack-prod`.`prod_marts`.`fct_oracle_neshu__conso_business_review`
    where company_id is not null
),

parent as (
    select idcompany as to_field
    from `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__company`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


