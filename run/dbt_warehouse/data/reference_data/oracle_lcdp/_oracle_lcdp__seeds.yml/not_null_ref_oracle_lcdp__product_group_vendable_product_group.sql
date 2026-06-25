
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select product_group
from `evs-datastack-prod`.`prod_reference`.`ref_oracle_lcdp__product_group_vendable`
where product_group is null



  
  
      
    ) dbt_internal_test