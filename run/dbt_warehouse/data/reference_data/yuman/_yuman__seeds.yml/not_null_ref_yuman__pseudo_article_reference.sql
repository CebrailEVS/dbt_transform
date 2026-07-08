
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select reference
from `evs-datastack-prod`.`prod_reference`.`ref_yuman__pseudo_article`
where reference is null



  
  
      
    ) dbt_internal_test