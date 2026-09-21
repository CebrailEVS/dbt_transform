





with validation_errors as (

    select
        idtask, idtax
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task_has_amount`
    group by idtask, idtax
    having count(*) > 1

)

select *
from validation_errors


