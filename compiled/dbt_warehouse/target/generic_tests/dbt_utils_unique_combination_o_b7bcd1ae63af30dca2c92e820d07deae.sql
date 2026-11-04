





with validation_errors as (

    select
        mois, company_id
    from `evs-datastack-prod`.`prod_marts`.`fct_neshu__pnl_client_mensuel`
    group by mois, company_id
    having count(*) > 1

)

select *
from validation_errors


