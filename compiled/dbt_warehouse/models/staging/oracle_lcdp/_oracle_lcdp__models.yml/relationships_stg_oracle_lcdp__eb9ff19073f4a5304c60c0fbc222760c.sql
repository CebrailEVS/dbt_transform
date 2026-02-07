
    
    

with child as (
    select idtask_type as from_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task`
    where idtask_type is not null
),

parent as (
    select idtask_type as to_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task_type`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


