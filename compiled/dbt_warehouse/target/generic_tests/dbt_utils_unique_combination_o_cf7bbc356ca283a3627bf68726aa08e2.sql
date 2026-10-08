





with validation_errors as (

    select
        mois, company_id, product_code
    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__neshu_disponibilite_depot_mensuel`
    group by mois, company_id, product_code
    having count(*) > 1

)

select *
from validation_errors


