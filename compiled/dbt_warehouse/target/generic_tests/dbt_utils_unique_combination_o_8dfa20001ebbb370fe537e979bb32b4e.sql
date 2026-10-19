





with validation_errors as (

    select
        snapshot_date, entity_type, id_entity, product_code
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__stock_theorique`
    group by snapshot_date, entity_type, id_entity, product_code
    having count(*) > 1

)

select *
from validation_errors


