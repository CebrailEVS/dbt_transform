
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select ar_ref
from `evs-datastack-prod`.`prod_raw`.`dbo_f_article`
where ar_ref is null



  
  
      
    ) dbt_internal_test