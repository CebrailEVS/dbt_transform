





with validation_errors as (

    select
        idcontract, idlabel
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__label_has_contract`
    group by idcontract, idlabel
    having count(*) > 1

)

select *
from validation_errors


