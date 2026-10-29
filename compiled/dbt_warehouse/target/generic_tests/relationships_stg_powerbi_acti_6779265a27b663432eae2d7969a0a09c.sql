
    
    

with child as (
    select dataset_id as from_field
    from (select * from `evs-datastack-prod`.`prod_staging`.`stg_powerbi_activity__events` where created_at >= timestamp_sub(current_timestamp(), interval 3 day)) dbt_subquery
    where dataset_id is not null
),

parent as (
    select dataset_id as to_field
    from `evs-datastack-prod`.`prod_staging`.`stg_powerbi_activity__datasets`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


