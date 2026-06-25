
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select product_name
from `evs-datastack-prod`.`prod_reference`.`ref_oracle_neshu__product_packaging`
where product_name is null



  
  
      
    ) dbt_internal_test