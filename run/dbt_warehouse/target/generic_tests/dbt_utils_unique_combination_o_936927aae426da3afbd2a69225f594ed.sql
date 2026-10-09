
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        mois_date, code_analytique, code_analytique_bu, numero_compte_general
    from `evs-datastack-prod`.`prod_marts`.`fct_finance__pnl_section_mensuel`
    group by mois_date, code_analytique, code_analytique_bu, numero_compte_general
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test