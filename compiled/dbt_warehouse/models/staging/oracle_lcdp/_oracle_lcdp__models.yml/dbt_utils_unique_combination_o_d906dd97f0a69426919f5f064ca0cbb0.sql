





with validation_errors as (

    select
        idcompany, idlabel
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label_company`
    group by idcompany, idlabel
    having count(*) > 1

)

select *
from validation_errors


