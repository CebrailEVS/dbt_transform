
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select quantity
from `evs-datastack-prod`.`prod_marts`.`fct_neshu__consommation`
where quantity is null



  
  
      
    ) dbt_internal_test