





with validation_errors as (

    select
        company_id, product_id
    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__prevision_demande_neshu`
    group by company_id, product_id
    having count(*) > 1

)

select *
from validation_errors


