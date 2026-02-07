
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select idproduct
from `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__product`
where idproduct is null



  
  
      
    ) dbt_internal_test