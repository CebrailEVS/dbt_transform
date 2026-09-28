





with validation_errors as (

    select
        ar_ref, ct_num
    from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_artfourniss`
    group by ar_ref, ct_num
    having count(*) > 1

)

select *
from validation_errors


