
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select l_code
from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__v_label_company`
where l_code is null



  
  
      
    ) dbt_internal_test