
    
    

with child as (
    select idtreatment_by_batch as from_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_historic`
    where idtreatment_by_batch is not null
),

parent as (
    select idtreatment_by_batch as to_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__treatment_by_batch`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


