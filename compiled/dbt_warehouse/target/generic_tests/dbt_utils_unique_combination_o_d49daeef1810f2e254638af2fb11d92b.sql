





with validation_errors as (

    select
        idtask, idresources, task_status
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__task_has_resources`
    group by idtask, idresources, task_status
    having count(*) > 1

)

select *
from validation_errors


