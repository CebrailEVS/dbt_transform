
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select idunit_type
from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__unit_type`
where idunit_type is null



  
  
      
    ) dbt_internal_test