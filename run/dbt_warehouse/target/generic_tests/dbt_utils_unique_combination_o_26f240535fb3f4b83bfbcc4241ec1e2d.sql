
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        depot_id, reference, n_serie
    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__nunshen_stock_lot`
    group by depot_id, reference, n_serie
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test