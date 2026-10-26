
    
    

with all_values as (

    select
        intervention_state as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_neshu__intervention_technique`
    group by intervention_state

)

select *
from all_values
where value_field not in (
    'REALISEE','NON_REALISEE','EN_COURS','EN_PAUSE','PLANIFIEE','DEMANDE_OUVERTE','DEMANDE_REJETEE','AUTRE'
)


