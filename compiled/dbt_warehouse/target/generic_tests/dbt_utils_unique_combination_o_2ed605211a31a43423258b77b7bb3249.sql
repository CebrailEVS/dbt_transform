





with validation_errors as (

    select
        code_analytique
    from `evs-datastack-prod`.`prod_app_compta`.`app_compta__section_analytique`
    group by code_analytique
    having count(*) > 1

)

select *
from validation_errors


