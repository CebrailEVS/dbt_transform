
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        date_inventaire, depot_id
    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__nunshen_inventaire`
    group by date_inventaire, depot_id
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test