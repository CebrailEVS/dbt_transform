





with validation_errors as (

    select
        _dlt_id
    from `evs-datastack-prod`.`prod_staging`.`stg_yuman_evs_sftp__stock_theorique`
    group by _dlt_id
    having count(*) > 1

)

select *
from validation_errors


