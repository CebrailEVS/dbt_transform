





with validation_errors as (

    select
        numero_ecriture_comptable, numero_plan_analytique, numero_ligne_analytique
    from `evs-datastack-prod`.`prod_marts`.`fct_mssql_sage__pnl_bu`
    group by numero_ecriture_comptable, numero_plan_analytique, numero_ligne_analytique
    having count(*) > 1

)

select *
from validation_errors


