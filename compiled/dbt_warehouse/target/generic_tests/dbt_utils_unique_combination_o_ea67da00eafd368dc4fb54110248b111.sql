





with validation_errors as (

    select
        idlabel_family, code
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__label`
    group by idlabel_family, code
    having count(*) > 1

)

select *
from validation_errors


