





with validation_errors as (

    select
        mouvement_date, device_id, product_id
    from `evs-datastack-prod`.`prod_marts`.`fct_lcdp__mouvement_produit`
    group by mouvement_date, device_id, product_id
    having count(*) > 1

)

select *
from validation_errors


