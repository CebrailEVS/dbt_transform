
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        device_id, date_debut_passage_appro, product_id
    from `evs-datastack-prod`.`prod_marts`.`fct_neshu__chargement_consommation`
    group by device_id, date_debut_passage_appro, product_id
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test