
    
    

with all_values as (

    select
        type as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_staging`.`stg_powerbi_activity__workspaces`
    group by type

)

select *
from all_values
where value_field not in (
    'Workspace'
)


