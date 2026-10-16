
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select code_type
from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__contact_type`
where code_type is null



  
  
      
    ) dbt_internal_test