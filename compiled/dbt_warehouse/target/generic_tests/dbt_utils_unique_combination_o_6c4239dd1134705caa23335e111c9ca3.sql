





with validation_errors as (

    select
        activity_id, activity_type
    from `evs-datastack-prod`.`prod_staging`.`stg_nesp_co__activite`
    group by activity_id, activity_type
    having count(*) > 1

)

select *
from validation_errors


