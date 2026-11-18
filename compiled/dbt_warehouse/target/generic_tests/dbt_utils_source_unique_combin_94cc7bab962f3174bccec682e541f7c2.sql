





with validation_errors as (

    select
        snapshot_date, opportunity
    from (select * from `evs-datastack-prod`.`prod_raw`.`nesp_co_opportunite` where opportunity != 'Result') dbt_subquery
    group by snapshot_date, opportunity
    having count(*) > 1

)

select *
from validation_errors


