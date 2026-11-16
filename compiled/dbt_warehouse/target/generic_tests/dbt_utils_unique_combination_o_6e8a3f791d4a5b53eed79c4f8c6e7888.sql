





with validation_errors as (

    select
        entity_code, product_code, snapshot_date
    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__neshu_inventaire_remise_zero`
    group by entity_code, product_code, snapshot_date
    having count(*) > 1

)

select *
from validation_errors


