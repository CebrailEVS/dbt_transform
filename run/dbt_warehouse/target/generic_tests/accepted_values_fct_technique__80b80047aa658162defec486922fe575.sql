
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

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
    'VALIDATED','NOT VALIDATED','NOT DEFINED','UNTRACKABLE','MISSING_TARIF','NOT_BILLABLE'
)



  
  
      
    ) dbt_internal_test