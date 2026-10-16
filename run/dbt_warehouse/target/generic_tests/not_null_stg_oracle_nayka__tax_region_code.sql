
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select code
from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__tax_region`
where code is null



  
  
      
    ) dbt_internal_test