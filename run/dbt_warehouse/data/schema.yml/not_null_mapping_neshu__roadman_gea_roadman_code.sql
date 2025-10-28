
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select roadman_code
from `evs-datastack-prod`.`prod_reference`.`mapping_neshu__roadman_gea`
where roadman_code is null



  
  
      
    ) dbt_internal_test