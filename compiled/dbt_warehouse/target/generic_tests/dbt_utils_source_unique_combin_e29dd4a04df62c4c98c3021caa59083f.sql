





with validation_errors as (

    select
        _dlt_load_id, ar_ref, de_no
    from `evs-datastack-prod`.`prod_raw`.`dbo_f_artstock`
    group by _dlt_load_id, ar_ref, de_no
    having count(*) > 1

)

select *
from validation_errors


