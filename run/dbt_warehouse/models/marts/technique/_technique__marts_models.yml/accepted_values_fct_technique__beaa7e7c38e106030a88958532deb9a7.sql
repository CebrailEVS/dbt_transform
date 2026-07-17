
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        intervention_state as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_technique__suivi_partenaire`
    group by intervention_state

)

select *
from all_values
where value_field not in (
    'REALISEE','NON_REALISEE','EN_PAUSE','EN_COURS','PLANIFIEE','DEMANDE_OUVERTE','DEMANDE_REJETEE','AUTRE'
)



  
  
      
    ) dbt_internal_test