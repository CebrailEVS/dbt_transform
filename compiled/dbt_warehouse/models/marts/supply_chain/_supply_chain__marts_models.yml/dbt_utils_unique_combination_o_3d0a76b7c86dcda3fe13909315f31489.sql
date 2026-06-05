





with validation_errors as (

    select
        id_entity, product_code, date_system
    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__stock_lcdp`
    group by id_entity, product_code, date_system
    having count(*) > 1

)

select *
from validation_errors


