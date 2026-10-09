
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        a_facturer_retraite as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_technique__intervention_retraitee`
    group by a_facturer_retraite

)

select *
from all_values
where value_field not in (
    'OUI','NON'
)



  
  
      
    ) dbt_internal_test