





with validation_errors as (

    select
        activite_date, report_id
    from `evs-datastack-prod`.`prod_marts`.`fct_bi__activite_rapport_jour`
    group by activite_date, report_id
    having count(*) > 1

)

select *
from validation_errors


