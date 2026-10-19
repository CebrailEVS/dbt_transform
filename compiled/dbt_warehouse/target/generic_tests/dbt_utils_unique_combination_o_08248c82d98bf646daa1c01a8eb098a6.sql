





with validation_errors as (

    select
        mois_cible, company_id, product_id
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__erreur_prevision`
    group by mois_cible, company_id, product_id
    having count(*) > 1

)

select *
from validation_errors


