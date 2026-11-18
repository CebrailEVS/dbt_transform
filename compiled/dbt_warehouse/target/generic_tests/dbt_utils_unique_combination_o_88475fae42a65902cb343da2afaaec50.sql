





with validation_errors as (

    select
        act_id, act_type
    from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_co__activites`
    group by act_id, act_type
    having count(*) > 1

)

select *
from validation_errors


