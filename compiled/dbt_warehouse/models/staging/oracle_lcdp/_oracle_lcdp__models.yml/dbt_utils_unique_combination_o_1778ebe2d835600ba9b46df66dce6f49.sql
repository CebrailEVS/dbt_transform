





with validation_errors as (

    select
        idcontact, iddevice
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__contact_has_device`
    group by idcontact, iddevice
    having count(*) > 1

)

select *
from validation_errors


