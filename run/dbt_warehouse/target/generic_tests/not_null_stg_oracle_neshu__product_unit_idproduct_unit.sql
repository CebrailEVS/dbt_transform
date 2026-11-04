
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select idproduct_unit
from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__product_unit`
where idproduct_unit is null



  
  
      
    ) dbt_internal_test