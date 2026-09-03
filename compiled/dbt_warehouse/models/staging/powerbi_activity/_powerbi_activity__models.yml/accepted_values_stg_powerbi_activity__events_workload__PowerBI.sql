
    
    

with all_values as (

    select
        workload as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_staging`.`stg_powerbi_activity__events`
    group by workload

)

select *
from all_values
where value_field not in (
    'PowerBI'
)


