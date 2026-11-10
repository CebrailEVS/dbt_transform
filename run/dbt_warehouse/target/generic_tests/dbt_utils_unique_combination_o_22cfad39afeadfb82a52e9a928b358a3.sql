
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        snapshot_date, device_id
    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__lcdp_machine_fin_mois`
    group by snapshot_date, device_id
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test