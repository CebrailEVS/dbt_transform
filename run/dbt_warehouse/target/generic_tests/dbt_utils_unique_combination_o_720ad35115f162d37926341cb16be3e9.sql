
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        product_type, company_code, annee_chgt, quinzaine_chgt
    from `evs-datastack-prod`.`prod_marts`.`fct_neshu__chargement_quinzaine`
    group by product_type, company_code, annee_chgt, quinzaine_chgt
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test