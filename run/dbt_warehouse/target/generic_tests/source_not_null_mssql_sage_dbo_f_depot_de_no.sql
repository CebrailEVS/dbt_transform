
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select de_no
from `evs-datastack-prod`.`prod_raw`.`dbo_f_depot`
where de_no is null



  
  
      
    ) dbt_internal_test