
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        n_planning, piece_ref_nomad
    from `evs-datastack-prod`.`prod_marts`.`fct_technique__piece_detachee_pricing_nespresso`
    group by n_planning, piece_ref_nomad
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test