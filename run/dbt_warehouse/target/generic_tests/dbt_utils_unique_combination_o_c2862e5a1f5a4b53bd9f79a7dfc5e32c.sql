
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        scenario, annee, mois, bu, kpi
    from `evs-datastack-prod`.`prod_marts`.`fct_finance__pnl_bu`
    group by scenario, annee, mois, bu, kpi
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test