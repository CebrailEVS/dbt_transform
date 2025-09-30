
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select company_info
from `evs-datastack-prod`.`prod_marts`.`fct_oracle_neshu__pa_business_review`
where company_info is null



  
  
      
    ) dbt_internal_test