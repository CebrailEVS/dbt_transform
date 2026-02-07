
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select idproduct_type
from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__product_type`
where idproduct_type is null



  
  
      
    ) dbt_internal_test