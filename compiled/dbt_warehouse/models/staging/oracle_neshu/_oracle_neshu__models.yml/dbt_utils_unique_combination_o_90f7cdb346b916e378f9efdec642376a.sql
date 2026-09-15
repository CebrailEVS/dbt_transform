





with validation_errors as (

    select
        idstring, langage_code
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__string`
    group by idstring, langage_code
    having count(*) > 1

)

select *
from validation_errors


