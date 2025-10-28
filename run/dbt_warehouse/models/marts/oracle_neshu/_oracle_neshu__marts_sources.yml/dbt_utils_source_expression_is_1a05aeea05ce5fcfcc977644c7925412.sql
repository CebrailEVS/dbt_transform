
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_oracle_neshu__passages_appro`

where not(passage_duration_min >= 0 OR passage_duration_min IS NULL)


  
  
      
    ) dbt_internal_test