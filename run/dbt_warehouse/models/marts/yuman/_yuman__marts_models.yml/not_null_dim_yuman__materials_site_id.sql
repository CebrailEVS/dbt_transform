
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select site_id
from `evs-datastack-prod`.`prod_marts`.`dim_yuman__materials`
where site_id is null



  
  
      
    ) dbt_internal_test