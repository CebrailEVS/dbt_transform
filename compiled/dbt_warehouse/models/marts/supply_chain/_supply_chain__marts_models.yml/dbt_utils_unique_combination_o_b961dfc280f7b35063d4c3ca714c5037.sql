





with validation_errors as (

    select
        mois, company_id, product_code
    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__disponibilite_article_neshu_mensuel`
    group by mois, company_id, product_code
    having count(*) > 1

)

select *
from validation_errors


