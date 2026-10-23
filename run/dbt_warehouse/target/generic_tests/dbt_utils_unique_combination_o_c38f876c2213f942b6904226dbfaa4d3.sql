
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        company_id, product_id, demande_mois
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__demande_mensuelle`
    group by company_id, product_id, demande_mois
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test