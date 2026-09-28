





with validation_errors as (

    select
        do_type, do_piece
    from `evs-datastack-prod`.`prod_raw`.`dbo_f_docentete`
    group by do_type, do_piece
    having count(*) > 1

)

select *
from validation_errors


