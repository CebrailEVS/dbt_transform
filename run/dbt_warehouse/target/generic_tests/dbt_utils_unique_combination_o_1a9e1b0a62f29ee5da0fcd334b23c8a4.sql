
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        commande_numero
    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__nunshen_commande_delai`
    group by commande_numero
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test