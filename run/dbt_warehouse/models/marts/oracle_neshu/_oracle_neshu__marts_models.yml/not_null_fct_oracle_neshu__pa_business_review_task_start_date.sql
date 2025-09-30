
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select task_start_date
from `evs-datastack-prod`.`prod_marts`.`fct_oracle_neshu__pa_business_review`
where task_start_date is null



  
  
      
    ) dbt_internal_test