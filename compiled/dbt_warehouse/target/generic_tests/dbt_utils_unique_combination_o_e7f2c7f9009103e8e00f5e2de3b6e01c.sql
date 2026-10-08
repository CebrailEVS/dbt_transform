





with validation_errors as (

    select
        mois, product_reference, technician_id
    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__technique_conso_nespresso_mensuelle`
    group by mois, product_reference, technician_id
    having count(*) > 1

)

select *
from validation_errors


