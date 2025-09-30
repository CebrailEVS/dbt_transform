
    
    

with child as (
    select idresources as from_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_has_resources`
    where idresources is not null
),

parent as (
    select idresources as to_field
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__resources`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


