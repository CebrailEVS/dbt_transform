
    
    

with child as (
    select idtask_status as from_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task`
    where idtask_status is not null
),

parent as (
    select idtask_status as to_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task_status`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


