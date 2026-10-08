





with validation_errors as (

    select
        mois, depot, reference
    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__technique_rupture_depot_mois`
    group by mois, depot, reference
    having count(*) > 1

)

select *
from validation_errors


