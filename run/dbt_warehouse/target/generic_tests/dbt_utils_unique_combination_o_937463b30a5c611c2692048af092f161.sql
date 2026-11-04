
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        annee, mois, reference
    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__nunshen_vente_mensuelle`
    group by annee, mois, reference
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test