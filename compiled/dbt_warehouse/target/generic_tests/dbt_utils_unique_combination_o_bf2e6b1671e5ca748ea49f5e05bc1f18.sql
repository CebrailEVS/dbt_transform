





with validation_errors as (

    select
        idproduct, idlabel
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__label_product`
    group by idproduct, idlabel
    having count(*) > 1

)

select *
from validation_errors


