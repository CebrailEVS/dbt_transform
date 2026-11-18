





with validation_errors as (

    select
        snapshot_date, activity_id, activity_type
    from `evs-datastack-prod`.`prod_raw`.`nesp_co_activite`
    group by snapshot_date, activity_id, activity_type
    having count(*) > 1

)

select *
from validation_errors


