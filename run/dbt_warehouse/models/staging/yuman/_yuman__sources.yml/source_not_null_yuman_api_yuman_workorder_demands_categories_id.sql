
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select id
from `evs-datastack-prod`.`prod_raw`.`yuman_workorder_demands_categories`
where id is null



  
  
      
    ) dbt_internal_test