
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select passage_start_datetime
from `evs-datastack-prod`.`prod_marts`.`fct_oracle_neshu__passages_appro`
where passage_start_datetime is null



  
  
      
    ) dbt_internal_test