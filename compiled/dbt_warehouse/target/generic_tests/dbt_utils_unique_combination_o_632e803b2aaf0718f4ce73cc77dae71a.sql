





with validation_errors as (

    select
        execution_id, node_id
    from `evs-datastack-prod`.`prod_marts`.`fct_plateforme__resultat_dbt`
    group by execution_id, node_id
    having count(*) > 1

)

select *
from validation_errors


