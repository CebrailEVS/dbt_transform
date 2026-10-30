





with validation_errors as (

    select
        execution_id, source_name, table_name
    from `evs-datastack-prod`.`prod_marts`.`fct_plateforme__fraicheur_source`
    group by execution_id, source_name, table_name
    having count(*) > 1

)

select *
from validation_errors


