





with validation_errors as (

    select
        reference, composant_reference
    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__nunshen_nomenclature`
    group by reference, composant_reference
    having count(*) > 1

)

select *
from validation_errors


