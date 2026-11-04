
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select is_client_neshu
from `evs-datastack-prod`.`prod_marts`.`dim_neshu__company`
where is_client_neshu is null



  
  
      
    ) dbt_internal_test