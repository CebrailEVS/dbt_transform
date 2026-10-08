





with validation_errors as (

    select
        company_id, product_id
    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__neshu_classification_article`
    group by company_id, product_id
    having count(*) > 1

)

select *
from validation_errors


