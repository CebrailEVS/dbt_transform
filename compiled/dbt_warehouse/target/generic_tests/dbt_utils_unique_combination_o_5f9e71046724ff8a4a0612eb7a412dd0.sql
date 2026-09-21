





with validation_errors as (

    select
        idlabel, idtask_has_product
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_has_thp`
    group by idlabel, idtask_has_product
    having count(*) > 1

)

select *
from validation_errors


