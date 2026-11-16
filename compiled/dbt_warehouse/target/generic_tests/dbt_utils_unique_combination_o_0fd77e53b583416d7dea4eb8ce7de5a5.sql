





with validation_errors as (

    select
        mois, device_id
    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__nayka_equipement_fin_mois`
    group by mois, device_id
    having count(*) > 1

)

select *
from validation_errors


