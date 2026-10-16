





with validation_errors as (

    select
        idtask, idresources
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__task_has_resources`
    group by idtask, idresources
    having count(*) > 1

)

select *
from validation_errors


