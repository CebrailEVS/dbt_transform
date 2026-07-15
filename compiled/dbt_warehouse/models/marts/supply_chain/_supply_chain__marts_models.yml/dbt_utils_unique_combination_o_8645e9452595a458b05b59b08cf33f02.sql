





with validation_errors as (

    select
        mois_cible, company_id, product_id
    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__erreur_prevision_neshu`
    group by mois_cible, company_id, product_id
    having count(*) > 1

)

select *
from validation_errors


