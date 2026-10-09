
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select dataset_id
from `evs-datastack-prod`.`prod_marts`.`dim_bi__rapport`
where dataset_id is null



  
  
      
    ) dbt_internal_test