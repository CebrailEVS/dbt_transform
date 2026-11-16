
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        code_analytique
    from `evs-datastack-prod`.`prod_app_compta`.`app_compta__section_analytique`
    group by code_analytique
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test