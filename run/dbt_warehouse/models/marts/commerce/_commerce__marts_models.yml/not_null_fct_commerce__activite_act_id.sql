
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select act_id
from `evs-datastack-prod`.`prod_marts`.`fct_commerce__activite`
where act_id is null



  
  
      
    ) dbt_internal_test