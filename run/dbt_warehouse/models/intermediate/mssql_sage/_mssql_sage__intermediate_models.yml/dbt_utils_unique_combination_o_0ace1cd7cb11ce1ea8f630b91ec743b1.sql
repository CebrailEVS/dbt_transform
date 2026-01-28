
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        numero_ecriture_comptable, numero_plan_analytique, numero_ligne_analytique
    from `evs-datastack-prod`.`prod_intermediate`.`int_mssql_sage__pnl_bu`
    group by numero_ecriture_comptable, numero_plan_analytique, numero_ligne_analytique
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test