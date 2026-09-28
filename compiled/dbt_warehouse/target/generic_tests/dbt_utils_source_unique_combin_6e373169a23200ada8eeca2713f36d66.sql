





with validation_errors as (

    select
        fa_code_famille, fa_type
    from `evs-datastack-prod`.`prod_raw`.`dbo_f_famille`
    group by fa_code_famille, fa_type
    having count(*) > 1

)

select *
from validation_errors


