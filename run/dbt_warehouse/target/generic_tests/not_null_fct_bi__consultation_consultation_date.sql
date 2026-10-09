
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select consultation_date
from `evs-datastack-prod`.`prod_marts`.`fct_bi__consultation`
where consultation_date is null



  
  
      
    ) dbt_internal_test