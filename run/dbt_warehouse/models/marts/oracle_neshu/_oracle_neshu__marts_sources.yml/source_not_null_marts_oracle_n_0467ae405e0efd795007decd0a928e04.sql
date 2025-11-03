
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select loaded_at_utc
from `evs-datastack-prod`.`prod_marts`.`fct_oracle_neshu__passages_appro`
where loaded_at_utc is null



  
  
      
    ) dbt_internal_test