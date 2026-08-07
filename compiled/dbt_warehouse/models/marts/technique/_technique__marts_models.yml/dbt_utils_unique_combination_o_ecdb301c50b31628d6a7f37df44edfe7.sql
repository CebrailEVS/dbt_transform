





with validation_errors as (

    select
        periode_credit, intervention_fautive_id
    from `evs-datastack-prod`.`prod_marts`.`fct_technique__credit_repair_warranty`
    group by periode_credit, intervention_fautive_id
    having count(*) > 1

)

select *
from validation_errors


