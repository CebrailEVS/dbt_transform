
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        n_planning, code_article
    from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__articles_dedup`
    group by n_planning, code_article
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test