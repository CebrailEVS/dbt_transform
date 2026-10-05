
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        mouvement_date, device_id, product_id, resources_roadman_id
    from `evs-datastack-prod`.`prod_marts`.`fct_lcdp__mouvement_produit`
    group by mouvement_date, device_id, product_id, resources_roadman_id
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test