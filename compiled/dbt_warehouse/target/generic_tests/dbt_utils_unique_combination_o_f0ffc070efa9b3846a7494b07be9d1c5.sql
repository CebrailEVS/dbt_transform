





with validation_errors as (

    select
        extracted_at, cb_marq
    from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_artstock`
    group by extracted_at, cb_marq
    having count(*) > 1

)

select *
from validation_errors


