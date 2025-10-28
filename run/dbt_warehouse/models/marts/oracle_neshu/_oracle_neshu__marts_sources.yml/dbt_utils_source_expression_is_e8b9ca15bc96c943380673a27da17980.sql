
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_oracle_neshu__passages_appro`

where not(passage_start_datetime <= passage_end_datetime)


  
  
      
    ) dbt_internal_test