
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select is_mandatory_article
from `evs-datastack-prod`.`prod_staging`.`stg_yuman__products`
where is_mandatory_article is null



  
  
      
    ) dbt_internal_test