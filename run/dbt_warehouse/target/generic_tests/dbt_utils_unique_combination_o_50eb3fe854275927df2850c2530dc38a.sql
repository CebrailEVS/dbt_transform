
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        n_piece, reference
    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__nunshen_ordre_production`
    group by n_piece, reference
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test