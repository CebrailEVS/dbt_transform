
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        num_intervention, code_article
    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__technique_conso_nespresso_detail`
    group by num_intervention, code_article
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test