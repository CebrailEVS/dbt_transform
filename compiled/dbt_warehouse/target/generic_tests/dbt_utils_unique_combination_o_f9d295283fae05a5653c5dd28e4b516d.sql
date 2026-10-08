





with validation_errors as (

    select
        stock_date, depot_id, reference
    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__nunshen_stock_photo`
    group by stock_date, depot_id, reference
    having count(*) > 1

)

select *
from validation_errors


