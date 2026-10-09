
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select third
from `evs-datastack-prod`.`prod_marts`.`dim_commerce__client`
where third is null



  
  
      
    ) dbt_internal_test