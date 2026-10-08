





with validation_errors as (

    select
        entity_code, product_code, date_system
    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__neshu_inventaire_transition`
    group by entity_code, product_code, date_system
    having count(*) > 1

)

select *
from validation_errors


