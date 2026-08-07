





with validation_errors as (

    select
        intervention_fautive_id
    from `evs-datastack-prod`.`prod_marts`.`fct_technique__credit_repair_warranty`
    group by intervention_fautive_id
    having count(*) > 1

)

select *
from validation_errors


