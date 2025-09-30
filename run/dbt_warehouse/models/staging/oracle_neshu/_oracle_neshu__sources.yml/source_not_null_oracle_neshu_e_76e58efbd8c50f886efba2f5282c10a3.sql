
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select idresources_type
from `evs-datastack-prod`.`prod_raw`.`evs_resources_type`
where idresources_type is null



  
  
      
    ) dbt_internal_test