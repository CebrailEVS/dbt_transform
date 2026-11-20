
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        code_article
    from `evs-datastack-prod`.`prod_app_suivi_tech`.`app_suivi_tech__technique_piece_nespresso`
    group by code_article
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test