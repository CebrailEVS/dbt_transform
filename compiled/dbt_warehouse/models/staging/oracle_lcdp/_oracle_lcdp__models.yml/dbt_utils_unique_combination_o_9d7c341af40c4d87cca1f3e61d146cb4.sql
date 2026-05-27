





with validation_errors as (

    select
        product_id, idlabel
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label_product`
    group by product_id, idlabel
    having count(*) > 1

)

select *
from validation_errors


