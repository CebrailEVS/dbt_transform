
    
    

with all_values as (

    select
        command as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_staging`.`stg_dbt_artifacts__runner_execution`
    group by command

)

select *
from all_values
where value_field not in (
    'build','snapshot'
)


