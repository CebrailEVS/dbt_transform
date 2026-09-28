





with validation_errors as (

    select
        extracted_at, ar_ref, de_no
    from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_artstock`
    group by extracted_at, ar_ref, de_no
    having count(*) > 1

)

select *
from validation_errors


