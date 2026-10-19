





with validation_errors as (

    select
        date_calcul, company_id, product_code
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__couverture_stock`
    group by date_calcul, company_id, product_code
    having count(*) > 1

)

select *
from validation_errors


