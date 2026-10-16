





with validation_errors as (

    select
        idtask, idlabel
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__label_has_task`
    group by idtask, idlabel
    having count(*) > 1

)

select *
from validation_errors


