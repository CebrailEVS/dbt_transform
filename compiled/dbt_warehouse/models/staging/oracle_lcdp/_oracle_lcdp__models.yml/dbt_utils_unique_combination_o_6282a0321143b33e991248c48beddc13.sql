





with validation_errors as (

    select
        iddevice, idlabel
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label_device`
    group by iddevice, idlabel
    having count(*) > 1

)

select *
from validation_errors


