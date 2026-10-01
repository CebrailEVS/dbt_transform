





with validation_errors as (

    select
        extracted_at, dl_no_in, dl_no_out, ls_no_serie
    from `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_lotserie`
    group by extracted_at, dl_no_in, dl_no_out, ls_no_serie
    having count(*) > 1

)

select *
from validation_errors


