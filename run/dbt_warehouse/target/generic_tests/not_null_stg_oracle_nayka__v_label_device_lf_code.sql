
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select lf_code
from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__v_label_device`
where lf_code is null



  
  
      
    ) dbt_internal_test