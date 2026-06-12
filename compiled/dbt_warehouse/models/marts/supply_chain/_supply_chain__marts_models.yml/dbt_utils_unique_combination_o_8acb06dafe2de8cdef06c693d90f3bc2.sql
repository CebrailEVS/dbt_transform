





with validation_errors as (

    select
        date_calcul, company_id, product_code
    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__couverture_stock_neshu`
    group by date_calcul, company_id, product_code
    having count(*) > 1

)

select *
from validation_errors


