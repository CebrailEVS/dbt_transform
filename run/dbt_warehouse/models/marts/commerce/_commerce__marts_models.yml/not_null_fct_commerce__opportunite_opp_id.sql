
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select opp_id
from `evs-datastack-prod`.`prod_marts`.`fct_commerce__opportunite`
where opp_id is null



  
  
      
    ) dbt_internal_test