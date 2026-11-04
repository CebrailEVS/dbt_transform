





with validation_errors as (

    select
        scenario, annee, mois, bu, kpi
    from `evs-datastack-prod`.`prod_marts`.`fct_finance__pnl_bu`
    group by scenario, annee, mois, bu, kpi
    having count(*) > 1

)

select *
from validation_errors


