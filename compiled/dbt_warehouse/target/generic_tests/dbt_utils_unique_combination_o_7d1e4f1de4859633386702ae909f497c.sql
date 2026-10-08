





with validation_errors as (

    select
        mois_cible, company_id, product_id
    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__neshu_erreur_prevision`
    group by mois_cible, company_id, product_id
    having count(*) > 1

)

select *
from validation_errors


