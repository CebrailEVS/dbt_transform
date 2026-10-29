





with validation_errors as (

    select
        execution_name, unique_id
    from `evs-datastack-prod`.`prod_intermediate`.`int_dbt_artifacts__node_result`
    group by execution_name, unique_id
    having count(*) > 1

)

select *
from validation_errors


