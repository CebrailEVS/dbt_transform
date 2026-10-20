





with validation_errors as (

    select
        calendrier, jour
    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__general_calendrier_ouvre`
    group by calendrier, jour
    having count(*) > 1

)

select *
from validation_errors


