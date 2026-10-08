





with validation_errors as (

    select
        stock_date, depot, reference
    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__technique_rupture_depot_courant`
    group by stock_date, depot, reference
    having count(*) > 1

)

select *
from validation_errors


