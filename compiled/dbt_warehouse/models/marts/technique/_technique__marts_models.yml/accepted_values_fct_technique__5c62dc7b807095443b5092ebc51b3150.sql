
    
    

with all_values as (

    select
        statut_facturation_effectif as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_technique__intervention_retraitee`
    group by statut_facturation_effectif

)

select *
from all_values
where value_field not in (
    'VALIDATED','NOT VALIDATED','NOT DEFINED'
)


