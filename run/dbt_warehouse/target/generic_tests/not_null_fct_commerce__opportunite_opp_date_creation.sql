
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select opp_date_creation
from `evs-datastack-prod`.`prod_marts`.`fct_commerce__opportunite`
where opp_date_creation is null



  
  
      
    ) dbt_internal_test