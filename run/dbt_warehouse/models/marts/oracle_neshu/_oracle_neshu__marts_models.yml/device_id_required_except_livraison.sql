
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_oracle_neshu__conso_business_review`

where not((device_id IS NOT NULL) OR (data_source = 'LIVRAISON'))


  
  
      
    ) dbt_internal_test