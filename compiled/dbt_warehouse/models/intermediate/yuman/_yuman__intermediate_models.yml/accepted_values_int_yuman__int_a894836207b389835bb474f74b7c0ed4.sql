
    
    

with all_values as (

    select
        intervention_state as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_intermediate`.`int_yuman__interventions`
    group by intervention_state

)

select *
from all_values
where value_field not in (
    'REALISEE','NON_REALISEE','EN_PAUSE','EN_COURS','PLANIFIEE','DEMANDE_OUVERTE','DEMANDE_REJETEE','AUTRE'
)


