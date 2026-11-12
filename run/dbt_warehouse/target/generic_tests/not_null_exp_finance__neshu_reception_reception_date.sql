
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select reception_date
from `evs-datastack-prod`.`prod_export`.`exp_finance__neshu_reception`
where reception_date is null



  
  
      
    ) dbt_internal_test