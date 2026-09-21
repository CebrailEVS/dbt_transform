
    
    

with child as (
    select idtask as from_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_has_product`
    where idtask is not null
),

parent as (
    select idtask as to_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


