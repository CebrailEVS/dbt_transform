
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        stock_date, depot, reference
    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__technique_rupture_depot_jour_vide`
    group by stock_date, depot, reference
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test