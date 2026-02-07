
    
    

with child as (
    select idproduct as from_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task_has_product`
    where idproduct is not null
),

parent as (
    select idproduct as to_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__product`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


