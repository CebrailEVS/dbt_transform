
    
    

with child as (
    select device_id as from_field
    from `evs-datastack-prod`.`prod_marts`.`fct_technique__neshu_maintenance_preventives`
    where device_id is not null
),

parent as (
    select device_id as to_field
    from `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__device`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


