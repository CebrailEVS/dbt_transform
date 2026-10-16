
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select idpayment_type
from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__payment_type`
where idpayment_type is null



  
  
      
    ) dbt_internal_test