
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select report_name
from `evs-datastack-prod`.`prod_marts`.`fct_bi__consultation`
where report_name is null



  
  
      
    ) dbt_internal_test