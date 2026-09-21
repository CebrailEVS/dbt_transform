





with validation_errors as (

    select
        device_id
    from (select * from `evs-datastack-prod`.`snapshots`.`snap_lcdp__device` where dbt_valid_to is null) dbt_subquery
    group by device_id
    having count(*) > 1

)

select *
from validation_errors


