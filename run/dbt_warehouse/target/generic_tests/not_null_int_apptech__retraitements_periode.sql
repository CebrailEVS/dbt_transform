
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select periode
from `evs-datastack-prod`.`prod_intermediate`.`int_apptech__retraitements`
where periode is null



  
  
      
    ) dbt_internal_test