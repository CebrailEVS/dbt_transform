





with validation_errors as (

    select
        journee, rang_journee
    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__technique_mouvement_stock`
    group by journee, rang_journee
    having count(*) > 1

)

select *
from validation_errors


