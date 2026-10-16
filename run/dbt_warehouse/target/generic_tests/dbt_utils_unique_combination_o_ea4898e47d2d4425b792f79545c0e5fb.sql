
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        mois, company_id, product_code
    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__disponibilite_article_neshu_depot_mensuel`
    group by mois, company_id, product_code
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test