
    
    

with child as (
    select idtask_has_product as from_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_has_thp`
    where idtask_has_product is not null
),

parent as (
    select idtask_has_product as to_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_has_product`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


