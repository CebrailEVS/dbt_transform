
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select is_vendable
from `evs-datastack-prod`.`prod_reference`.`ref_oracle_lcdp__product_group_vendable`
where is_vendable is null



  
  
      
    ) dbt_internal_test