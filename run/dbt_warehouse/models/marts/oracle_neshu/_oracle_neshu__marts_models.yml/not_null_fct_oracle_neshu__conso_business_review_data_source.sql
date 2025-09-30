
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select data_source
from `evs-datastack-prod`.`prod_marts`.`fct_oracle_neshu__conso_business_review`
where data_source is null



  
  
      
    ) dbt_internal_test