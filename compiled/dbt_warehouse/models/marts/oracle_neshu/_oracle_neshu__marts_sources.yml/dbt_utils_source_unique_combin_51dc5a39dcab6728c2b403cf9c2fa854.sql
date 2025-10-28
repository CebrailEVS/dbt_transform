





with validation_errors as (

    select
        task_id, roadman_code
    from `evs-datastack-prod`.`prod_marts`.`fct_oracle_neshu__passages_appro`
    group by task_id, roadman_code
    having count(*) > 1

)

select *
from validation_errors


