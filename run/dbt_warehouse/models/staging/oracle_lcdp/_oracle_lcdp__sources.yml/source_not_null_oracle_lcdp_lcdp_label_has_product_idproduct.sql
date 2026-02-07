
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select idproduct
from `evs-datastack-prod`.`prod_raw`.`lcdp_label_has_product`
where idproduct is null



  
  
      
    ) dbt_internal_test